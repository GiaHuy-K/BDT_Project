"""Ingest the Grupo Bimbo Kaggle dataset into MinIO raw storage.

Steps (re-runnable, idempotent):
  1. Download the competition zip with the Kaggle API if it is not in data/.
  2. Stream every CSV out of the nested zips (no full extraction to disk).
  3. Upload to s3://<RAW_BUCKET>/grupo_bimbo/<table>/<table>.csv, skipping files
     whose sha256 already matches the object in MinIO.
  4. Write a manifest (sizes, row counts, checksums) to MinIO and logs/.

Usage:  python ingestion/ingest.py [--force]
"""
import argparse
import datetime as dt
import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

from dotenv import load_dotenv
from minio import Minio
from minio.commonconfig import CopySource

ROOT = Path(__file__).resolve().parents[1]
COMPETITION = "grupo-bimbo-inventory-demand"
SKIP = {"sample_submission.csv"}  # Kaggle submission format, not used
PREFIX = "grupo_bimbo"


class HashingReader(io.RawIOBase):
    """Wraps a stream, computing sha256 and counting lines as it is read."""

    def __init__(self, raw):
        self.raw, self.sha, self.lines = raw, hashlib.sha256(), 0

    def readable(self):
        return True

    def read(self, n=-1):
        chunk = self.raw.read(n)
        self.sha.update(chunk)
        self.lines += chunk.count(b"\n")
        return chunk


def ensure_zip(zip_path: Path) -> None:
    data_dir = zip_path.parent / "grupo-bimbo-inventory-demand"
    if zip_path.exists() or data_dir.exists():
        return
    print(f"[ingest] {zip_path} not found, downloading from Kaggle...")
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["kaggle", "competitions", "download", "-c", COMPETITION, "-p", str(zip_path.parent)],
        check=True,
    )


def iter_csvs(zip_path: Path):
    """Yield (csv_name, size, stream) for every CSV inside directory or nested zips."""
    data_dir = zip_path.parent / "grupo-bimbo-inventory-demand"
    if data_dir.is_dir():
        for p in data_dir.glob("**/*.csv"):
            if p.is_file() and p.name not in SKIP:
                f = open(p, "rb")
                try:
                    yield p.name, p.stat().st_size, f
                finally:
                    f.close()
        return

    with zipfile.ZipFile(zip_path) as outer:
        for name in outer.namelist():
            with zipfile.ZipFile(outer.open(name)) as inner:
                for info in inner.infolist():
                    if info.filename.endswith(".csv") and info.filename not in SKIP:
                        with inner.open(info) as stream:
                            yield info.filename, info.file_size, stream


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="re-upload even if checksum matches")
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    zip_path = ROOT / os.getenv("DATA_ZIP", "data/grupo-bimbo-inventory-demand.zip")
    bucket = os.getenv("RAW_BUCKET", "raw")
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )
    for b in (bucket, os.getenv("PROCESSED_BUCKET", "processed")):
        if not client.bucket_exists(b):
            client.make_bucket(b)

    ensure_zip(zip_path)
    started = dt.datetime.now(dt.timezone.utc)
    manifest = {"source": f"kaggle:{COMPETITION}", "started_at": started.isoformat(), "files": []}

    for csv_name, size, stream in iter_csvs(zip_path):
        table = csv_name.removesuffix(".csv")
        key = f"{PREFIX}/{table}/{csv_name}"
        reader = HashingReader(stream)
        existing = None
        try:
            existing = client.stat_object(bucket, key).metadata.get("x-amz-meta-sha256")
        except Exception:
            pass

        if existing and not args.force:
            # Still hash locally to confirm the stored object is the same file.
            while reader.read(8 << 20):
                pass
            if reader.sha.hexdigest() == existing:
                print(f"[ingest] skip {key} (unchanged)")
                manifest["files"].append({"key": key, "bytes": size, "rows": reader.lines - 1,
                                          "sha256": existing, "status": "skipped"})
                continue
            print(f"[ingest] {key} differs from stored copy; re-run with --force", file=sys.stderr)
            return 1

        print(f"[ingest] upload {key} ({size / 1e6:.1f} MB)")
        # sha256 is only known after streaming, so upload first then attach it via copy.
        client.put_object(bucket, key, reader, length=size, part_size=64 << 20,
                          content_type="text/csv")
        digest = reader.sha.hexdigest()
        client.copy_object(bucket, key, CopySource(bucket, key),
                           metadata={"sha256": digest}, metadata_directive="REPLACE")
        manifest["files"].append({"key": key, "bytes": size, "rows": reader.lines - 1,
                                  "sha256": digest, "status": "uploaded"})

    manifest["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    body = json.dumps(manifest, indent=2).encode()
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    client.put_object(bucket, f"{PREFIX}/_manifests/ingest_{stamp}.json", io.BytesIO(body), len(body),
                      content_type="application/json")
    (ROOT / "logs").mkdir(exist_ok=True)
    (ROOT / "logs" / f"ingest_{stamp}.json").write_bytes(body)
    print(f"[ingest] done: {len(manifest['files'])} files, manifest ingest_{stamp}.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
