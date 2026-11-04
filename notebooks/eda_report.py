"""EDA figures and numbers, from the tables written by spark_jobs/eda.py to logs/eda/.

Usage:  python notebooks/eda_report.py
Writes: docs/eda/figures/*.png and docs/eda/eda_summary.json (every number quoted in EDA_report.md).
Needs:  pandas, numpy, scipy, scikit-learn, matplotlib (host side, see requirements.txt).
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter
from scipy.stats import gaussian_kde, spearmanr
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
IN = ROOT / "logs" / "eda"
OUT = ROOT / "docs" / "eda"
FIG = OUT / "figures"
SEED = 42

# Palette checked with the dataviz validator; text stays in ink colors, never the series color
TEAL, CORAL, VIOLET, AMBER, PINK, GREEN = "#008f7f", "#d9622b", "#7a5cc7", "#c98500", "#d8689a", "#4f8a1f"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
SEGMENT_COLORS = {
    "VIP / High-Value": TEAL, "Regular / Loyal": CORAL, "Standard": VIOLET,
    "Unknown / Unidentified": AMBER, "Occasional / Churn Risk": PINK, "High Return Risk": GREEN,
}
plt.rcParams.update({
    "font.family": "Calibri", "font.size": 11, "axes.edgecolor": MUTED, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "axes.titlelocation": "left",
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": False, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
    "figure.facecolor": "white", "savefig.facecolor": "white", "legend.frameon": False,
})
S: dict = {}


def load_json(name: str):
    return json.loads((IN / f"{name}.json").read_text(encoding="utf-8"))


def save(fig, name: str) -> None:
    fig.tight_layout()
    fig.savefig(FIG / f"{name}.png", dpi=200)
    plt.close(fig)
    print("figure", name)


def weighted_quantiles(values, counts, qs) -> list:
    order = np.argsort(values)
    v, c = np.asarray(values)[order], np.asarray(counts)[order]
    cum = np.cumsum(c) / c.sum()
    return [float(v[np.searchsorted(cum, q)]) for q in qs]


def box_stats(values, counts, label: str) -> dict:
    q1, med, q3 = weighted_quantiles(values, counts, [.25, .5, .75])
    iqr = q3 - q1
    inside = np.asarray(values)[np.asarray(values) <= q3 + 1.5 * iqr]
    return {"label": label, "q1": q1, "med": med, "q3": q3,
            "whislo": float(np.min(values)), "whishi": float(inside.max()), "fliers": []}


def step1_2() -> None:
    tables = load_json("tables")
    S["tables"] = {k: {"rows": v["rows"], "columns": len(v["columns"]),
                       "nulls": sum(c["nulls"] for c in v["columns"])} for k, v in tables.items()}
    S["train_distinct"] = {c["column"]: c["distinct"] for c in tables["train"]["columns"]}
    S["duplicate_keys"] = load_json("duplicate_keys")
    S["descriptive"] = load_json("descriptive")
    for col in ("Demanda_uni_equil", "Venta_uni_hoy", "Dev_uni_proxima"):
        d = pd.read_csv(IN / f"values_{col}.csv").groupby(col)["count"].sum().reset_index()
        qs = [.01, .05, .25, .5, .75, .9, .95, .99, .999]
        S[f"quantiles_{col}"] = dict(zip(map(str, qs), weighted_quantiles(d[col], d["count"], qs)))
        S[f"zero_pct_{col}"] = float(d.loc[d[col] == 0, "count"].sum() / d["count"].sum() * 100)


def step3() -> None:
    d = pd.read_csv(IN / "values_Demanda_uni_equil.csv")
    total = d.groupby("Demanda_uni_equil")["count"].sum()
    mean, med = S["descriptive"]["Demanda_uni_equil"]["mean"], S["quantiles_Demanda_uni_equil"]["0.5"]
    p999 = S["quantiles_Demanda_uni_equil"]["0.999"]

    # Histogram with labeled bins: single values up to 5, then wider ranges. Equal-width bars over
    # unequal ranges would mislead on a log axis (a 3-value bin would look like the mode).
    bins = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 10), (11, 25), (26, 50), (51, 100), (101, 250), (251, 1000), (1001, 5000)]
    labels = ["0", "1", "2", "3", "4", "5", "6-10", "11-25", "26-50", "51-100", "101-250", "251-1k", "1k-5k"]
    shares = [total[(total.index >= a) & (total.index <= b)].sum() / total.sum() * 100 for a, b in bins]
    S["hist_bins_pct"] = dict(zip(labels, np.round(shares, 3).tolist()))
    fig, ax = plt.subplots(figsize=(7.4, 4.1))
    colors = [CORAL if a <= med <= b else TEAL for a, b in bins]
    ax.bar(labels, shares, color=colors, width=.72)
    for i, v in enumerate(shares):
        ax.text(i, v + .4, f"{v:.1f}%" if v >= .05 else "<0.01%", ha="center", fontsize=8.5, color=INK2)
    ax.tick_params(axis="x", labelsize=9)
    ax.set_xlabel("Nhu cầu của một dòng (đơn vị; khoảng rộng dần về bên phải)")
    ax.set_ylabel("% số dòng")
    ax.set_ylim(0, max(shares) * 1.18)
    ax.grid(axis="y")
    ax.text(.98, .93, f"Trung vị = {med:.0f} (cột cam)" + chr(10) + f"Trung bình = {mean:.2f}" + chr(10) + f"p99.9 = {p999:.0f}" + chr(10) + "Lớn nhất = 5,000",
            transform=ax.transAxes, ha="right", va="top", fontsize=10.5, color=INK2, linespacing=1.5)
    ax.set_title("Phân phối nhu cầu: lệch phải rất mạnh")
    save(fig, "03_hist_demand")

    # Boxplot per channel, exact from value counts. Fliers hidden: there are millions of them.
    order = d.groupby("Canal_ID")["count"].sum().sort_values(ascending=False).index
    stats = []
    for ch in order:
        g = d[d.Canal_ID == ch]
        stats.append(box_stats(g["Demanda_uni_equil"] + 1, g["count"], f"Kênh {ch}"))
    fig, ax = plt.subplots(figsize=(7.4, 4.1))
    ax.bxp(stats, vert=False, showfliers=False, widths=.55, patch_artist=True,
           boxprops={"facecolor": "#c9ebe5", "edgecolor": TEAL}, medianprops={"color": CORAL, "lw": 2},
           whiskerprops={"color": MUTED}, capprops={"color": MUTED})
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xticks([1, 2, 6, 11, 31, 101, 301, 1001], ["0", "1", "5", "10", "30", "100", "300", "1,000"])
    ax.minorticks_off()
    ax.grid(axis="x")
    ax.set_xlabel("Nhu cầu của một dòng (đơn vị, trục log)")
    for i, ch in enumerate(order):
        n = d.loc[d.Canal_ID == ch, "count"].sum()
        ax.text(1.0, i + 1, f"{n / 1e6:.2f} tr dòng" if n > 1e5 else f"{n:,.0f} dòng",
                transform=ax.get_yaxis_transform(), ha="right", va="center", fontsize=8.5, color=INK2)
    ax.set_title("Mức nhu cầu khác hẳn nhau giữa các kênh")
    save(fig, "04_box_channel")
    S["median_by_channel"] = {int(ch): weighted_quantiles(d[d.Canal_ID == ch]["Demanda_uni_equil"],
                                                          d[d.Canal_ID == ch]["count"], [.5])[0] for ch in order}

    # KDE on log10(demand + 1), weighted by exact counts (a light jitter breaks up integer spikes)
    fig, ax = plt.subplots(figsize=(7.4, 4.1))
    xs = np.linspace(0, 3.6, 400)
    rng = np.random.default_rng(SEED)
    for ch, color, name in [(1, TEAL, "Kênh 1"), (4, CORAL, "Kênh 4"), (2, VIOLET, "Kênh 2"), (5, AMBER, "Kênh 5")]:
        g = d[d.Canal_ID == ch]
        pts = np.log10(g["Demanda_uni_equil"].values + 1 + rng.uniform(0, .5, len(g)))
        ys = gaussian_kde(pts, weights=g["count"].values, bw_method=.25)(xs)
        ax.plot(xs, ys, color=color, lw=2, label=name)
        peak = xs[np.argmax(ys)]
        ax.text(peak, ys.max() + .03, name, ha="center", color=INK2, fontsize=10)
    ax.set_xticks(np.log10([1, 4, 11, 31, 101, 301, 1001]), ["0", "3", "10", "30", "100", "300", "1,000"])
    ax.set_xlabel("Nhu cầu của một dòng (đơn vị, trục log)")
    ax.set_ylabel("Mật độ")
    ax.grid(axis="y")
    ax.legend(loc="upper right")
    ax.set_title("Mật độ theo kênh: mỗi kênh là một phân phối riêng")
    save(fig, "05_kde_channel")


def step4(c: pd.DataFrame) -> None:
    state = c.groupby("State")["net_revenue"].sum().sort_values(ascending=False)
    state = state / state.sum() * 100
    S["top_states_pct"] = state.head(5).round(2).to_dict()
    S["top4_states_pct"] = float(state.head(4).sum())
    fig, ax = plt.subplots(figsize=(5.8, 4.1))
    top = state.head(10)
    ax.barh(top.index.str.title(), top.values, color=TEAL, height=.62)
    ax.invert_yaxis()
    ax.grid(axis="x")
    ax.set_xlabel("% doanh thu thuần")
    for y, v in enumerate(top.values):
        ax.text(v + .2, y, f"{v:.1f}%", va="center", fontsize=9, color=INK2)
    ax.set_xlim(0, top.max() * 1.15)
    ax.set_title("Count plot: 10 bang lớn nhất trong 32 bang")
    save(fig, "06_state_bar")

    brand = c.groupby("brand")["net_revenue"].sum().sort_values(ascending=False)
    brand = brand / brand.sum() * 100
    S["brand_pct"] = brand.head(6).round(2).to_dict()
    parts = pd.concat([brand.head(4), pd.Series({"Khác": brand.iloc[4:].sum()})])
    fig, ax = plt.subplots(figsize=(5.2, 4.1))
    wedges, _ = ax.pie(parts.values, colors=[TEAL, CORAL, VIOLET, AMBER, MUTED], startangle=90, counterclock=False,
                       wedgeprops={"width": .42, "edgecolor": "white", "linewidth": 2})
    ax.legend(wedges, [f"{k}  {v:.1f}%" for k, v in parts.items()], loc="center left", bbox_to_anchor=(.98, .5))
    ax.set_title("Pie chart: thị phần brand")
    save(fig, "07_brand_donut")

    # Lorenz curves: the honest view of "a few clients and products make most of the money"
    clients = pd.read_csv(IN / "clients_all.csv", usecols=["net_revenue", "active_weeks"])
    products = pd.read_csv(IN / "products.csv", usecols=["net_revenue"])
    fig, ax = plt.subplots(figsize=(6.4, 4.1))
    for frame, color, name, key in [(clients, TEAL, "Khách hàng", "clients"), (products, CORAL, "Sản phẩm", "products")]:
        v = np.sort(frame["net_revenue"].clip(lower=0).values)
        cum = np.cumsum(v) / v.sum()
        x = np.linspace(0, 1, len(v))
        gini = 1 - 2 * np.trapezoid(cum, x)
        ax.plot(x * 100, cum * 100, color=color, lw=2, label=f"{name} (Gini {gini:.2f})")
        desc = np.cumsum(v[::-1]) / v.sum()
        S[f"concentration_{key}"] = {"gini": float(gini), "n": int(len(v)),
                                     **{f"top_{int(q * 100)}pct": float(desc[max(int(len(v) * q) - 1, 0)]) for q in (.01, .1, .2)}}
    ax.plot([0, 100], [0, 100], color=MUTED, ls="--", lw=1)
    ax.text(62, 67, "Phân bố đều", color=INK2, fontsize=9, rotation=35)
    ax.set_xlabel("% khách hàng / sản phẩm (sắp từ nhỏ đến lớn)")
    ax.set_ylabel("% doanh thu thuần tích lũy")
    ax.grid(True)
    ax.legend(loc="upper left")
    ax.set_title("Đường Lorenz: doanh thu tập trung vào số ít")
    save(fig, "08_lorenz")
    S["active_weeks"] = {int(k): int(v) for k, v in clients["active_weeks"].value_counts().sort_index().items()}


def step5() -> None:
    cols = ["Venta_uni_hoy", "Venta_hoy", "Dev_uni_proxima", "Dev_proxima", "Demanda_uni_equil"]
    labels = ["Bán (đv)", "Bán (peso)", "Trả (đv)", "Trả (peso)", "Nhu cầu"]
    pearson = pd.DataFrame(np.eye(5), index=cols, columns=cols)
    for key, value in load_json("pearson_all_rows").items():
        a, b = key.split("|")
        pearson.loc[a, b] = pearson.loc[b, a] = value
    sample = pd.read_csv(IN / "row_sample.csv")
    spearman = pd.DataFrame(spearmanr(sample[cols].values).statistic, index=cols, columns=cols)
    S["pearson"] = pearson.round(3).to_dict()
    S["spearman_sample"] = spearman.round(3).to_dict()
    S["row_sample_rows"] = int(len(sample))
    fig, axes = plt.subplots(1, 2, figsize=(10.6, 4.2))
    for ax, m, title in [(axes[0], pearson, "Pearson: toàn bộ 74.2 triệu dòng"),
                         (axes[1], spearman, f"Spearman: mẫu {len(sample):,} dòng")]:
        im = ax.imshow(m.values, vmin=-1, vmax=1, cmap="BrBG")
        ax.set_xticks(range(5), labels, rotation=30, ha="right")
        ax.set_yticks(range(5), labels)
        for i in range(5):
            for j in range(5):
                v = m.values[i, j]
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=10, color="white" if abs(v) > .6 else INK)
        ax.spines[:].set_visible(False)
        ax.tick_params(length=0)
        ax.set_title(title)
    fig.colorbar(im, ax=axes, fraction=.025, pad=.02)
    fig.savefig(FIG / "09_correlation.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print("figure 09_correlation")

    # Scatter: sold vs returned. Hexbin because 370k points would be a solid block.
    r = sample[sample.Dev_uni_proxima > 0]
    S["sample_rows_with_returns"] = int(len(r))
    S["sample_returns_gt_sales_pct"] = float((r.Dev_uni_proxima > r.Venta_uni_hoy).mean() * 100)
    fig, ax = plt.subplots(figsize=(5.6, 3.9))
    hb = ax.hexbin(r.Venta_uni_hoy + 1, r.Dev_uni_proxima, xscale="log", yscale="log", gridsize=34, bins="log",
                   cmap="BuGn", mincnt=1, linewidths=.2)
    lim = [1, 3000]
    ax.plot(lim, lim, color=CORAL, ls="--", lw=1.5)
    ax.text(600, 950, "trả = bán", color=INK2, fontsize=10, ha="right")
    ax.set_xlabel("Đơn vị bán + 1 (trục log)")
    ax.set_ylabel("Đơn vị trả (trục log)")
    fig.colorbar(hb, ax=ax, label="Số dòng (log)")
    ax.set_title("Scatter: bán và trả, chỉ các dòng có hàng trả")
    save(fig, "10_scatter_sold_returned")


def step6(c: pd.DataFrame) -> None:
    w = c.groupby("Semana")[["demand_units", "sales_units", "sales_pesos", "return_units", "return_pesos"]].sum()
    top = pd.read_csv(IN / "top_return_rows.csv").iloc[0]
    clean = w["return_units"].copy()
    clean[top.Semana] -= top.Dev_uni_proxima
    S["single_row"] = {k: (v.item() if hasattr(v, "item") else v) for k, v in top.items()}
    S["weekly"] = {
        "demand_m": (w.demand_units / 1e6).round(2).to_dict(),
        "return_units": w.return_units.astype(int).to_dict(),
        "return_units_without_row": clean.astype(int).to_dict(),
        "return_pct_units": (w.return_units / w.sales_units * 100).round(3).to_dict(),
        "return_pct_units_without_row": (clean / w.sales_units * 100).round(3).to_dict(),
        "return_pct_value": (w.return_pesos / w.sales_pesos * 100).round(3).to_dict(),
        "return_units_change_w3_w9_pct": float((w.return_units[9] / w.return_units[3] - 1) * 100),
        "return_units_change_without_row_pct": float((clean[9] / clean[3] - 1) * 100),
        "return_pesos_change_w3_w9_pct": float((w.return_pesos[9] / w.return_pesos[3] - 1) * 100),
    }
    S["overall_return_pct_units"] = float(w.return_units.sum() / w.sales_units.sum() * 100)
    S["overall_return_pct_units_without_row"] = float(clean.sum() / w.sales_units.sum() * 100)
    S["overall_return_pct_value"] = float(w.return_pesos.sum() / w.sales_pesos.sum() * 100)

    # Small multiples, not a dual axis: each measure keeps its own scale
    weeks = w.index.values
    fig, axes = plt.subplots(1, 3, figsize=(11.6, 3.7))
    axes[0].plot(weeks, w.demand_units / 1e6, color=TEAL, lw=2, marker="o", ms=6)
    axes[0].set_ylim(0, 95)
    axes[0].set_title("Nhu cầu (triệu đơn vị)")
    axes[1].plot(weeks, w.return_units / 1e6, color=CORAL, lw=2, marker="o", ms=6, label="Số liệu gốc")
    axes[1].plot(weeks, clean / 1e6, color=TEAL, lw=2, marker="o", ms=6, label="Bỏ 1 dòng 250,000")
    axes[1].set_ylim(0, 1.95)
    axes[1].legend(loc="lower left")
    axes[1].set_title("Hàng trả (triệu đơn vị)")
    axes[1].annotate("+27.6%", (9, w.return_units[9] / 1e6), textcoords="offset points", xytext=(-6, 7), ha="right", color=INK2)
    axes[1].annotate("+8.2%", (9, clean[9] / 1e6), textcoords="offset points", xytext=(-4, -16), ha="right", color=INK2)
    axes[2].plot(weeks, w.return_pesos / w.sales_pesos * 100, color=TEAL, lw=2, marker="o", ms=6)
    axes[2].set_ylim(0, 2.4)
    axes[2].set_title("Tỷ lệ trả theo giá trị (%)")
    for ax in axes:
        ax.set_xticks(weeks)
        ax.set_xlabel("Tuần")
        ax.grid(axis="y")
    save(fig, "11_time_series")

    lag = load_json("lag_correlation")
    present = {int(k): v for k, v in lag["series_by_weeks_present"].items()}
    total = sum(present.values())
    S["lag"] = {"rows": lag["rows"], "by_lag": lag["by_lag"], "series": total,
                "series_one_week_pct": present[1] / total * 100, "series_all_weeks_pct": present[7] / total * 100,
                "series_by_weeks": dict(sorted(present.items()))}
    fig, axes = plt.subplots(1, 2, figsize=(10.2, 3.9))
    x = [r["lag"] for r in lag["by_lag"]]
    y = [r["corr_log1p"] for r in lag["by_lag"]]
    axes[0].bar(x, y, color=TEAL, width=.62)
    axes[0].set_ylim(0, 1)
    axes[0].set_xlabel("Cách nhau bao nhiêu tuần")
    axes[0].set_ylabel("Tương quan của log(1 + nhu cầu)")
    for a, b in zip(x, y):
        axes[0].text(a, b + .02, f"{b:.2f}", ha="center", fontsize=10, color=INK2)
    axes[0].grid(axis="y")
    axes[0].set_title("Nhu cầu tuần này dự đoán tốt tuần sau")
    keys = sorted(present)
    axes[1].bar(keys, [present[k] / total * 100 for k in keys], color=TEAL, width=.62)
    axes[1].set_xlabel("Số tuần có giao dịch (trên 7 tuần)")
    axes[1].set_ylabel("% chuỗi cửa hàng × sản phẩm")
    for k in keys:
        axes[1].text(k, present[k] / total * 100 + .6, f"{present[k] / total * 100:.1f}%", ha="center", fontsize=10, color=INK2)
    axes[1].grid(axis="y")
    axes[1].set_title("Nhưng phần lớn chuỗi rất thưa")
    save(fig, "12_lag_sparsity")


def step7(c: pd.DataFrame) -> None:
    d = pd.read_csv(IN / "values_Demanda_uni_equil.csv")
    total = d.groupby("Demanda_uni_equil")["count"].sum()
    v, n = total.index.values, total.values
    q1, q3 = weighted_quantiles(v, n, [.25, .75])
    fence = q3 + 1.5 * (q3 - q1)
    p999 = S["quantiles_Demanda_uni_equil"]["0.999"]
    def share(threshold):
        mask = v > threshold
        return float(n[mask].sum() / n.sum() * 100), float((v[mask] * n[mask]).sum() / (v * n).sum() * 100)
    iqr_rows, iqr_units = share(fence)
    q_rows, q_units = share(249)
    S["outliers"] = {"q1": q1, "q3": q3, "iqr_fence": fence, "iqr_rows_pct": iqr_rows, "iqr_units_pct": iqr_units,
                     "pipeline_threshold": 249, "exact_p999": p999, "p999_rows_pct": q_rows, "p999_units_pct": q_units}
    ch = c.groupby("Canal_ID")[["rows", "outlier_rows", "outlier_units", "demand_units"]].sum()
    ch["pct"] = ch.outlier_rows / ch.rows * 100
    S["outliers"]["by_channel_pct"] = ch.pct.round(3).to_dict()
    S["outliers"]["by_channel_share_of_flagged_pct"] = (ch.outlier_rows / ch.outlier_rows.sum() * 100).round(1).to_dict()

    fig, axes = plt.subplots(1, 2, figsize=(10.2, 3.9))
    x = np.arange(2)
    axes[0].bar(x - .2, [iqr_rows, q_rows], width=.38, color=TEAL, label="% số dòng bị gắn cờ")
    axes[0].bar(x + .2, [iqr_units, q_units], width=.38, color=CORAL, label="% số đơn vị trong các dòng đó")
    axes[0].set_xticks(x, [f"Quy tắc IQR (> {fence:.0f})", "Phân vị 99.9% (> 249)"])
    for i, (a, b) in enumerate([(iqr_rows, iqr_units), (q_rows, q_units)]):
        axes[0].text(i - .2, a + 1, f"{a:.2f}%", ha="center", fontsize=10, color=INK2)
        axes[0].text(i + .2, b + 1, f"{b:.2f}%", ha="center", fontsize=10, color=INK2)
    axes[0].set_ylim(0, 78)
    axes[0].grid(axis="y")
    axes[0].legend(loc="upper right")
    axes[0].set_title("IQR gắn cờ quá nhiều dữ liệu bình thường")
    o = ch[ch.rows > 1000].sort_values("pct", ascending=False)
    axes[1].barh([f"Kênh {k}" for k in o.index], o.pct, color=TEAL, height=.62)
    axes[1].invert_yaxis()
    axes[1].grid(axis="x")
    axes[1].set_xlim(0, o.pct.max() * 1.18)
    for y, val in enumerate(o.pct):
        axes[1].text(val + .15, y, f"{val:.2f}%", va="center", fontsize=9.5, color=INK2)
    axes[1].set_xlabel("% số dòng của kênh có nhu cầu > 249")
    axes[1].set_title("Một ngưỡng chung không công bằng giữa các kênh")
    save(fig, "13_outliers")


def step8() -> None:
    # Spark returns rows in no fixed order; sort so the same seed always picks the same clients
    f = pd.read_csv(IN / "client_features.csv").sort_values("Cliente_ID").reset_index(drop=True)
    f["avg_price"] = f.sales_pesos / f.sales_units.replace(0, np.nan)
    f["return_share"] = f.return_units / f.sales_units.replace(0, np.nan)
    f = f.dropna(subset=["avg_price", "return_share"]).copy()
    features = pd.DataFrame({
        "log_dòng": np.log1p(f["rows"]), "log_sản_phẩm": np.log1p(f.n_products), "log_tuyến": np.log1p(f.n_routes),
        "tuần_hoạt_động": f.active_weeks, "log_đơn_vị_bán": np.log1p(f.sales_units),
        "log_doanh_thu": np.log1p(f.net_revenue.clip(lower=0)), "log_giá_tb": np.log1p(f.avg_price),
        "log_tỷ_lệ_trả": np.log1p(f.return_share),
    })
    X = StandardScaler().fit_transform(features)
    pca = PCA(random_state=SEED).fit(X)
    Z = pca.transform(X)
    evr = pca.explained_variance_ratio_
    S["pca"] = {"clients": int(len(f)), "explained_pct": (evr * 100).round(2).tolist(),
                "pc1_loadings": dict(zip(features.columns, pca.components_[0].round(3).tolist())),
                "pc2_loadings": dict(zip(features.columns, pca.components_[1].round(3).tolist()))}

    rng = np.random.default_rng(SEED)
    idx = rng.choice(len(f), 9000, replace=False)
    fig, axes = plt.subplots(1, 2, figsize=(10.8, 4.1), gridspec_kw={"width_ratios": [1.65, 1]})
    for seg, color in SEGMENT_COLORS.items():
        m = f.iloc[idx].segment.values == seg
        axes[0].scatter(Z[idx][m, 0], Z[idx][m, 1], s=7, color=color, alpha=.65, label=seg, linewidths=0)
    axes[0].set_xlabel(f"PC1 ({evr[0] * 100:.1f}%): quy mô mua hàng")
    axes[0].set_ylabel(f"PC2 ({evr[1] * 100:.1f}%)")
    axes[0].legend(markerscale=2.2, fontsize=8.5, loc="upper left", ncol=2, handletextpad=.1, columnspacing=.8)
    axes[0].set_ylim(top=Z[idx][:, 1].max() + (np.ptp(Z[idx][:, 1])) * .28)
    axes[0].grid(True)
    axes[0].set_title("PCA: 9,000 khách, màu theo phân khúc Q4")
    axes[1].bar(range(1, 9), evr * 100, color=TEAL, width=.65)
    for i, val in enumerate(evr[:4]):
        axes[1].text(i + 1, val * 100 + 1, f"{val * 100:.0f}%", ha="center", fontsize=9.5, color=INK2)
    axes[1].set_xlabel("Thành phần chính")
    axes[1].set_ylabel("% phương sai")
    axes[1].grid(axis="y")
    axes[1].set_title("Scree plot")
    save(fig, "14_pca")

    # Slide version: PCA alone, legend large enough to read at half-slide width
    fig, ax = plt.subplots(figsize=(5.9, 3.9))
    for seg, color in SEGMENT_COLORS.items():
        m = f.iloc[idx].segment.values == seg
        ax.scatter(Z[idx][m, 0], Z[idx][m, 1], s=7, color=color, alpha=.65, label=seg, linewidths=0)
    ax.set_xlabel(f"PC1 ({evr[0] * 100:.1f}%): quy mô mua hàng")
    ax.set_ylabel(f"PC2 ({evr[1] * 100:.1f}%): giá trung bình")
    ax.set_ylim(top=Z[idx][:, 1].max() + np.ptp(Z[idx][:, 1]) * .34)
    ax.legend(markerscale=2.4, fontsize=9, loc="upper left", ncol=2, handletextpad=.1, columnspacing=.8)
    ax.grid(True)
    ax.set_title("PCA: 9,000 khách, màu theo phân khúc Q4")
    save(fig, "14b_pca_scatter")

    idx2 = rng.choice(len(f), 6000, replace=False)
    emb = TSNE(n_components=2, perplexity=40, init="pca", learning_rate="auto", random_state=SEED).fit_transform(Z[idx2][:, :6])
    fig, ax = plt.subplots(figsize=(7.2, 4.3))
    for seg, color in SEGMENT_COLORS.items():
        m = f.iloc[idx2].segment.values == seg
        ax.scatter(emb[m, 0], emb[m, 1], s=7, color=color, alpha=.7, label=seg, linewidths=0)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines[:].set_visible(False)
    ax.legend(markerscale=2.2, fontsize=8.5, loc="center left", bbox_to_anchor=(1, .5))
    ax.set_title("t-SNE: 6,000 khách, màu theo phân khúc Q4")
    save(fig, "15_tsne")
    fig, ax = plt.subplots(figsize=(4.6, 3.9))
    for seg, color in SEGMENT_COLORS.items():
        m = f.iloc[idx2].segment.values == seg
        ax.scatter(emb[m, 0], emb[m, 1], s=6, color=color, alpha=.7, linewidths=0)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines[:].set_visible(False)
    ax.set_title("t-SNE: 6,000 khách, cùng bảng màu")
    save(fig, "15b_tsne_slide")

    # Does the data contain natural clusters? Silhouette near 1 = clear, near 0 = a continuous cloud.
    # Averaged over 5 client samples, because one sample moves the score by several hundredths.
    sil = {k: [] for k in range(2, 9)}
    seg_sil = []
    for trial in range(5):
        pick = np.random.default_rng(SEED + trial).choice(len(f), 20000, replace=False)
        for k in sil:
            labels = KMeans(n_clusters=k, n_init=5, random_state=SEED).fit_predict(X[pick])
            sil[k].append(float(silhouette_score(X[pick], labels, sample_size=8000, random_state=SEED)))
        codes = f.iloc[pick].segment.astype("category").cat.codes.values
        seg_sil.append(float(silhouette_score(X[pick], codes, sample_size=8000, random_state=SEED)))
    S["kmeans_silhouette"] = {k: {"mean": float(np.mean(v)), "min": min(v), "max": max(v)} for k, v in sil.items()}
    S["segment_silhouette_q4_rules"] = {"mean": float(np.mean(seg_sil)), "min": min(seg_sil), "max": max(seg_sil)}
    S["test_vs_train"] = load_json("test_vs_train")
    t = S["test_vs_train"]
    S["cold_start_pct"] = {"new_client": t["rows_new_client"] / t["rows"] * 100, "new_product": t["rows_new_product"] / t["rows"] * 100,
                           "new_pair_sample": t["pair_sample"]["rows_new_pair"] / t["pair_sample"]["rows"] * 100}


def main() -> None:
    FIG.mkdir(parents=True, exist_ok=True)
    cube = pd.read_csv(IN / "cube.csv")
    step1_2()
    step3()
    step4(cube)
    step5()
    step6(cube)
    step7(cube)
    step8()
    S["meta"] = load_json("meta")
    (OUT / "eda_summary.json").write_text(json.dumps(S, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print("summary written")


if __name__ == "__main__":
    main()
