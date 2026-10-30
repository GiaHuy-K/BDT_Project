"""Render the Q1-Q4 figures for the report and slides from the gold tables in PostgreSQL.

Run after ./run.sh has finished (containers up, gold.* exported):
    python docs/report/make_figures.py
Writes PNGs to docs/report/figures/. Every number is queried live, nothing is typed in.
"""
import csv
import io
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "figures"

# Reference palette (dataviz skill): categorical slots 1-2, text and grid tokens.
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK_2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def query(sql: str) -> list[dict]:
    out = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "bimbo", "-d", "bimbo",
         "--csv", "-c", sql],
        cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout
    return list(csv.DictReader(io.StringIO(out)))


def style(ax, title: str, subtitle: str) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK_2, labelsize=11, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold", color=INK, pad=28)
    ax.text(0, 1.02, subtitle, transform=ax.transAxes, fontsize=11, color=INK_2)


def save(fig, name: str) -> None:
    OUT.mkdir(exist_ok=True)
    fig.savefig(OUT / name, dpi=200, facecolor=SURFACE, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {OUT / name}")


def q1() -> None:
    rows = query(
        'SELECT "Semana" AS week, SUM(demand_units) AS demand, SUM(return_units) AS returns '
        'FROM gold.weekly_demand_by_state_channel GROUP BY 1 ORDER BY 1'
    )
    weeks = [int(r["week"]) for r in rows]
    demand = [int(r["demand"]) / 1e6 for r in rows]
    rate = [100 * int(r["returns"]) / int(r["demand"]) for r in rows]

    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.6), facecolor=SURFACE)
    a.plot(weeks, demand, color=BLUE, linewidth=2, marker="o", markersize=7)
    a.set_ylim(0, max(demand) * 1.15)
    style(a, "Nhu cầu ổn định", "Nhu cầu toàn quốc, triệu đơn vị / tuần")
    for x, y in [(weeks[0], demand[0]), (weeks[-1], demand[-1])]:
        a.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, 10),
                   ha="center", fontsize=11, color=INK)

    b.plot(weeks, rate, color=ORANGE, linewidth=2, marker="o", markersize=7)
    b.set_ylim(0, max(rate) * 1.25)
    style(b, "…nhưng tỷ lệ trả hàng tăng", "Hàng trả / nhu cầu, % theo tuần")
    for x, y in [(weeks[0], rate[0]), (weeks[-1], rate[-1])]:
        b.annotate(f"{y:.2f}%", (x, y), textcoords="offset points", xytext=(0, 10),
                   ha="center", fontsize=11, color=INK)
    for ax in (a, b):
        ax.set_xticks(weeks, [f"Tuần {w}" for w in weeks])
    fig.tight_layout(w_pad=4)
    save(fig, "q1_weekly_trend.png")


def q2() -> None:
    r = query(
        "SELECT 100.0*COUNT(*) FILTER (WHERE is_anomaly)/COUNT(*) FILTER (WHERE sales_units>=20) AS combos, "
        "100.0*SUM(sales_pesos) FILTER (WHERE is_anomaly)/SUM(sales_pesos) AS sales, "
        "100.0*SUM(return_pesos) FILTER (WHERE is_anomaly)/SUM(return_pesos) AS returns, "
        "MAX(threshold_2sigma) AS thr, COUNT(*) FILTER (WHERE is_anomaly) AS n "
        "FROM gold.return_rate_by_product_agency_route"
    )[0]
    labels = ["Số tổ hợp", "Doanh thu", "Giá trị hàng trả"]
    vals = [float(r["combos"]), float(r["sales"]), float(r["returns"])]

    fig, ax = plt.subplots(figsize=(12, 4.2), facecolor=SURFACE)
    colors = [INK_2, INK_2, ORANGE]
    ax.barh(labels[::-1], vals[::-1], color=colors[::-1], height=0.5)
    for i, v in enumerate(vals[::-1]):
        ax.text(v + 0.3, i, f"{v:.1f}%", va="center", fontsize=12, color=INK)
    ax.set_xlim(0, max(vals) * 1.2)
    style(ax, f"{int(r['n']):,} tổ hợp dị biệt gây ra {vals[2]:.1f}% giá trị hàng trả",
          f"Tỷ trọng của nhóm tổ hợp sản phẩm × agency × tuyến vượt ngưỡng μ+2σ = {100 * float(r['thr']):.1f}%")
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.tick_params(axis="y", labelsize=12, labelcolor=INK)
    save(fig, "q2_anomaly_share.png")


def q3() -> None:
    rows = query(
        "SELECT brand, 100.0*SUM(net_revenue)/SUM(SUM(net_revenue)) OVER () AS share "
        "FROM gold.product_ranking_by_state GROUP BY 1 ORDER BY 2 DESC"
    )
    top = rows[:5]
    other = sum(float(r["share"]) for r in rows[5:])
    labels = [r["brand"] for r in top] + [f"{len(rows) - 5} brand khác"]
    vals = [float(r["share"]) for r in top] + [other]

    fig, ax = plt.subplots(figsize=(12, 4.6), facecolor=SURFACE)
    colors = [BLUE] + [INK_2] * (len(vals) - 1)
    ax.barh(labels[::-1], vals[::-1], color=colors[::-1], height=0.55)
    for i, v in enumerate(vals[::-1]):
        ax.text(v + 0.6, i, f"{v:.1f}%", va="center", fontsize=12, color=INK)
    ax.set_xlim(0, max(vals) * 1.15)
    style(ax, f"Brand BIM chiếm {vals[0]:.1f}% doanh thu thuần toàn quốc",
          "Thị phần doanh thu thuần theo mã brand (tách từ tên sản phẩm), tuần 3–9")
    ax.grid(axis="y", visible=False)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.tick_params(axis="y", labelsize=12, labelcolor=INK)
    save(fig, "q3_brand_share.png")


def q4() -> None:
    rows = query(
        "SELECT segment, 100.0*COUNT(*)/SUM(COUNT(*)) OVER () AS clients, "
        "100.0*SUM(net_revenue)/SUM(SUM(net_revenue)) OVER () AS revenue "
        "FROM gold.client_segments GROUP BY 1 ORDER BY revenue DESC"
    )
    segs = [r["segment"] for r in rows]
    clients = [float(r["clients"]) for r in rows]
    revenue = [float(r["revenue"]) for r in rows]

    fig, ax = plt.subplots(figsize=(12, 5), facecolor=SURFACE)
    xs = range(len(segs))
    w = 0.38
    ax.bar([x - w / 2 - 0.01 for x in xs], clients, width=w, color=INK_2, label="% số điểm bán")
    ax.bar([x + w / 2 + 0.01 for x in xs], revenue, width=w, color=BLUE, label="% doanh thu thuần")
    for x, c, v in zip(xs, clients, revenue):
        ax.text(x - w / 2, c + 1, f"{c:.1f}", ha="center", fontsize=10.5, color=INK_2)
        ax.text(x + w / 2, v + 1, f"{v:.1f}", ha="center", fontsize=10.5, color=INK)
    ax.set_xticks(list(xs), [s.replace(" / ", "\n") for s in segs])
    ax.set_ylim(0, max(revenue) * 1.15)
    vip = segs.index("VIP / High-Value")
    style(ax, f"{clients[vip]:.1f}% điểm bán (VIP) tạo ra {revenue[vip]:.1f}% doanh thu thuần",
          "So sánh tỷ trọng số điểm bán và doanh thu thuần theo phân khúc, %")
    ax.legend(frameon=False, fontsize=11, loc="upper right", labelcolor=INK)
    save(fig, "q4_segments.png")


if __name__ == "__main__":
    q1()
    q2()
    q3()
    q4()
