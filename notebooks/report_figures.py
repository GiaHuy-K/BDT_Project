"""Draw the report figures from the gold tables in PostgreSQL (run after ./run.sh).

Usage:  python notebooks/report_figures.py
Reads the "-- name:" blocks in sql/analysis/report_figures.sql, prints every result,
and writes docs/report/figures/q1..q4_*.png for the blocks marked "(figure)".
"""
import csv
import io
import os
import re
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "report" / "figures"

# Reference chart palette (light surface): series 1 blue, series 2 orange
SURFACE, INK, INK2, MUTED, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
BLUE, ORANGE = "#2a78d6", "#eb6834"


def load_queries() -> dict:
    text = (ROOT / "sql" / "analysis" / "report_figures.sql").read_text(encoding="utf-8")
    blocks = re.split(r"^-- name: ", text, flags=re.M)[1:]
    return {b.split()[0]: b.split("\n", 1)[1] for b in blocks}


def query(sql: str) -> list:
    """Run SQL inside the postgres container, so a local PostgreSQL on port 5432 cannot interfere."""
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", os.getenv("POSTGRES_USER", "bimbo"),
         "-d", os.getenv("POSTGRES_DB", "bimbo"), "--csv", "-v", "ON_ERROR_STOP=1", "-c", sql],
        cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout
    rows = list(csv.reader(io.StringIO(out)))[1:]
    return [tuple(int(v) if re.fullmatch(r"-?\d+", v) else float(v) if re.fullmatch(r"-?\d+\.\d+", v) else v
                  for v in row) for row in rows]


def style(ax, title: str, xlabel: str = "") -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=12, fontweight="bold", pad=12)
    ax.set_xlabel(xlabel, color=INK2)
    ax.tick_params(colors=INK2, length=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(MUTED)
    ax.grid(axis="x" if ax.get_yaxis().get_inverted() else "y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def hbar(rows, title: str, xlabel: str, fmt: str, path: Path) -> None:
    labels, values = [r[0] for r in rows], [float(r[1]) for r in rows]
    fig, ax = plt.subplots(figsize=(7, 0.45 * len(rows) + 1.2), facecolor=SURFACE)
    ax.barh(labels, values, color=BLUE, height=0.6)
    ax.invert_yaxis()
    style(ax, title, xlabel)
    for y, v in enumerate(values):
        ax.text(v, y, " " + fmt.format(v), va="center", color=INK2, fontsize=9)
    ax.set_xlim(0, max(values) * 1.15)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def main() -> None:
    load_dotenv(ROOT / ".env")
    results = {}
    for name, sql in load_queries().items():
        results[name] = query(sql)
        print(f"== {name}")
        for row in results[name]:
            print("  ", row)
    OUT.mkdir(parents=True, exist_ok=True)

    # Q1: one series (national demand per week), y axis from zero so stability is not exaggerated
    weeks = [r[0] for r in results["q1_weekly"]]
    demand = [r[1] / 1e6 for r in results["q1_weekly"]]
    fig, ax = plt.subplots(figsize=(7, 3.4), facecolor=SURFACE)
    ax.plot(weeks, demand, color=BLUE, linewidth=2, marker="o", markersize=6)
    style(ax, "Q1 · Nhu cầu toàn quốc theo tuần (triệu đơn vị)", "Tuần (Semana)")
    ax.set_ylim(0, max(demand) * 1.15)
    low = demand.index(min(demand))
    ax.annotate(f"{demand[low]:.1f}M", (weeks[low], demand[low]), textcoords="offset points",
                xytext=(0, -16), ha="center", color=INK2, fontsize=9)
    ax.annotate(f"{max(demand):.1f}M", (weeks[demand.index(max(demand))], max(demand)),
                textcoords="offset points", xytext=(0, 8), ha="center", color=INK2, fontsize=9)
    fig.tight_layout()
    fig.savefig(OUT / "q1_weekly_demand.png", dpi=150)
    plt.close(fig)

    hbar(results["q2_by_brand"], "Q2 · Số tổ hợp sản phẩm×agency×tuyến dị biệt, theo brand",
         "Số tổ hợp vượt ngưỡng μ+2σ", "{:,.0f}", OUT / "q2_anomalies_by_brand.png")
    hbar(results["q3_brand_share"], "Q3 · Thị phần doanh thu thuần theo brand (toàn quốc)",
         "% doanh thu thuần", "{:.1f}%", OUT / "q3_brand_share.png")

    # Q4: two series per segment, legend + direct labels so identity is not color-only
    seg = results["q4_segments"]
    labels = [r[0] for r in seg]
    clients, revenue = [float(r[2]) for r in seg], [float(r[3]) for r in seg]
    fig, ax = plt.subplots(figsize=(7.5, 4), facecolor=SURFACE)
    y = range(len(seg))
    h = 0.38
    ax.barh([i - h / 2 for i in y], clients, height=h - 0.04, color=BLUE, label="% số khách")
    ax.barh([i + h / 2 for i in y], revenue, height=h - 0.04, color=ORANGE, label="% doanh thu thuần")
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    style(ax, "Q4 · % số khách và % doanh thu theo phân khúc", "%")
    for i, (c, r) in enumerate(zip(clients, revenue)):
        ax.text(c, i - h / 2, f" {c:.1f}%", va="center", color=INK2, fontsize=8)
        ax.text(r, i + h / 2, f" {r:.1f}%", va="center", color=INK2, fontsize=8)
    ax.set_xlim(0, max(clients + revenue) * 1.15)
    ax.legend(frameon=False, loc="lower right", labelcolor=INK2)
    fig.tight_layout()
    fig.savefig(OUT / "q4_segments.png", dpi=150)
    plt.close(fig)
    print(f"figures written to {OUT}")


if __name__ == "__main__":
    main()
