"""Figures for the forecast and per-channel outliers, from the JSON reports written by the pipeline.

Usage:  python notebooks/forecast_figures.py   (after ./run.sh)
Writes: docs/report/figures/forecast_accuracy.png, forecast_importance.png, outliers_by_channel.png
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"
FIG = ROOT / "docs" / "report" / "figures"
TEAL, CORAL, INK2, MUTED = "#008f7f", "#d9622b", "#52514e", "#898781"
plt.rcParams.update({
    "font.family": "Calibri", "font.size": 11, "axes.edgecolor": MUTED, "axes.labelcolor": INK2, "xtick.color": INK2,
    "ytick.color": INK2, "axes.titlelocation": "left", "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.spines.top": False, "axes.spines.right": False, "grid.color": "#e1e0d9", "axes.axisbelow": True,
    "figure.facecolor": "white", "savefig.facecolor": "white", "legend.frameon": False,
})
LABELS = {
    "pair_mean": "TB của khách × sản phẩm", "route_product_mean": "TB của tuyến × sản phẩm", "client_mean": "TB của khách",
    "agency_product_mean": "TB của agency × sản phẩm", "product_price": "Giá sản phẩm", "product_mean": "TB của sản phẩm",
    "pair_last": "Lần gần nhất", "channel_mean": "TB của kênh", "client_weeks": "Số tuần của khách",
    "pair_weeks": "Số tuần của cặp", "has_pair": "Có lịch sử cặp",
}


def latest(pattern: str) -> dict:
    files = sorted(LOGS.glob(pattern), key=lambda p: p.stat().st_mtime)
    return json.loads(files[-1].read_text(encoding="utf-8"))


def save(fig, name: str) -> None:
    fig.tight_layout()
    fig.savefig(FIG / f"{name}.png", dpi=200)
    plt.close(fig)
    print("figure", name)


def accuracy(r: dict) -> None:
    groups = ["Tất cả", "Cặp đã có lịch sử", "Cặp mới"]
    base = [r["rmsle"]["history_baseline"], r["rmsle_known_pair"]["history_baseline"], r["rmsle_new_pair"]["history_baseline"]]
    gbt = [r["rmsle"]["gbt"], r["rmsle_known_pair"]["gbt"], r["rmsle_new_pair"]["gbt"]]
    x = np.arange(3)
    fig, ax = plt.subplots(figsize=(6, 3.9))
    ax.bar(x - .2, base, .38, color=TEAL, label="Baseline: trung bình lịch sử")
    ax.bar(x + .2, gbt, .38, color=CORAL, label="GBT")
    for i in range(3):
        ax.text(i - .2, base[i] + .012, f"{base[i]:.3f}", ha="center", fontsize=10, color=INK2)
        ax.text(i + .2, gbt[i] + .012, f"{gbt[i]:.3f}", ha="center", fontsize=10, color=INK2)
    ax.set_xticks(x, groups)
    ax.set_ylabel("RMSLE (càng thấp càng tốt)")
    ax.set_ylim(0, max(base) * 1.32)
    ax.grid(axis="y")
    ax.legend(loc="upper left")
    ax.set_title("Sai số trên tuần 9, khách chưa dùng để huấn luyện")
    save(fig, "forecast_accuracy")


def importance(r: dict) -> None:
    items = sorted(r["feature_importance"].items(), key=lambda kv: kv[1], reverse=True)[:7]
    fig, ax = plt.subplots(figsize=(5.4, 3.9))
    ax.barh([LABELS[k] for k, _ in items], [v * 100 for _, v in items], color=TEAL, height=.62)
    ax.invert_yaxis()
    ax.grid(axis="x")
    for y, (_, v) in enumerate(items):
        ax.text(v * 100 + .6, y, f"{v * 100:.1f}%", va="center", fontsize=10, color=INK2)
    ax.set_xlim(0, items[0][1] * 118)
    ax.set_xlabel("% mức độ quan trọng trong mô hình")
    ax.set_title("Lịch sử của chính cặp quan trọng nhất")
    save(fig, "forecast_importance")


def outliers(q: dict) -> None:
    rows = [c for c in q["outliers_by_channel"] if c["rows"] > 1000]
    rows.sort(key=lambda c: c["rows"], reverse=True)
    y = np.arange(len(rows))
    glob = [c["flagged_global"] / c["rows"] * 100 for c in rows]
    chan = [c["flagged_channel"] / c["rows"] * 100 for c in rows]
    fig, ax = plt.subplots(figsize=(6.6, 3.9))
    ax.barh(y - .2, glob, .38, color=TEAL, label="Ngưỡng chung (> 249)")
    ax.barh(y + .2, chan, .38, color=CORAL, label="Ngưỡng riêng từng kênh")
    ax.set_yticks(y, [f"Kênh {c['Canal_ID']} (> {c['threshold']:,})" for c in rows])
    ax.invert_yaxis()
    for i in range(len(rows)):
        ax.text(glob[i] + .12, i - .2, f"{glob[i]:.2f}%", va="center", fontsize=9, color=INK2)
        ax.text(chan[i] + .12, i + .2, f"{chan[i]:.2f}%", va="center", fontsize=9, color=INK2)
    ax.set_xlim(0, max(glob) * 1.15)
    ax.grid(axis="x")
    ax.set_xlabel("% số dòng của kênh bị gắn cờ")
    ax.legend(loc="lower right")
    ax.set_title("Mỗi kênh giờ bị gắn cờ cùng một tỷ lệ")
    save(fig, "outliers_by_channel")


def main() -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    report = latest("forecast_report_*.json")
    accuracy(report)
    importance(report)
    outliers(latest("silver_quality_*.json"))


if __name__ == "__main__":
    main()
