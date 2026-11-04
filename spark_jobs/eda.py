"""EDA: profile the full dataset with Spark and write small tables for notebooks/eda_report.py.

Read-only: it reads bronze, silver and gold, and writes only under logs/eda/. It is not a step of
run.sh, so the pipeline and its timings do not change. Run after ./run.sh:

  docker compose exec -T spark-master /opt/spark/bin/spark-submit /opt/pipeline/spark_jobs/eda.py

Aggregates (exact, over all 74.2M rows): schema and missing values, descriptive statistics, value
counts per channel, a cube by week x channel x state x brand, product totals, Pearson correlations.
Row or client samples (deterministic): row_sample, client_features, and the lag / duplicate checks.
"""
from __future__ import annotations  # the Spark image ships Python 3.8

import csv
import json
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config, schemas
from common.spark_session import get_spark

OUT = "/opt/pipeline/logs/eda"
MEASURES = ["Venta_uni_hoy", "Venta_hoy", "Dev_uni_proxima", "Dev_proxima", "Demanda_uni_equil"]
ROW_KEY = ["Semana", "Agencia_ID", "Canal_ID", "Ruta_SAK", "Cliente_ID", "Producto_ID"]
SERIES_KEY = ["Agencia_ID", "Canal_ID", "Ruta_SAK", "Cliente_ID", "Producto_ID"]
ROW_SAMPLE_FRACTION = 0.005  # ~370k rows, enough for rank correlation and scatter plots
CLIENT_SAMPLE_MOD = 10       # 1 client in 10 (pmod on the ID, so the same clients every run)
SERIES_SAMPLE_MOD = 50       # 1 client in 50 for the self-joins


def log(msg: str) -> None:
    print(f"[eda] {msg}", flush=True)


def save_json(name: str, obj) -> None:
    with open(f"{OUT}/{name}.json", "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    log(f"wrote {name}.json")


def save_csv(name: str, df: DataFrame) -> None:
    """Stream through the driver: the logs/ mount is reliable there and no pandas is needed."""
    n = 0
    with open(f"{OUT}/{name}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(df.columns)
        for row in df.toLocalIterator():
            w.writerow(row)
            n += 1
    log(f"wrote {name}.csv: {n:,} rows")


def profile_tables(spark) -> dict:
    """Step 1: size, types, missing values and distinct counts of every bronze table."""
    out = {}
    for table in config.TABLES:
        df = spark.read.parquet(f"{config.BRONZE}/{table}")
        cols = [c for c in df.columns if c not in ("_source_file", "_ingested_at")]
        nulls = df.agg(*[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in cols]).first().asDict()
        rows = df.count()
        info = []
        for field in schemas.BY_TABLE[table].fields:
            c = field.name
            info.append({
                "column": c,
                "type": field.dataType.simpleString(),
                "nulls": nulls[c],
                "distinct": df.select(c).distinct().count(),
            })
        out[table] = {"rows": rows, "columns": info}
        log(f"profiled {table}: {rows:,} rows")
    return out


def descriptive(fact: DataFrame) -> dict:
    """Step 2: moments of the measures. skewness/kurtosis are Spark's (excess kurtosis: normal = 0)."""
    cols = {m: F.col(m) for m in MEASURES}
    cols["unit_price"] = F.col("unit_price")
    cols["return_rate"] = F.col("return_rate")
    exprs = []
    for name, c in cols.items():
        exprs += [
            F.count(c).alias(f"{name}__count"), F.mean(c).alias(f"{name}__mean"),
            F.stddev(c).alias(f"{name}__std"), F.variance(c).alias(f"{name}__var"),
            F.min(c).alias(f"{name}__min"), F.max(c).alias(f"{name}__max"),
            F.skewness(c).alias(f"{name}__skew"), F.kurtosis(c).alias(f"{name}__kurt"),
        ]
    flat = fact.agg(*exprs).first().asDict()
    stats = {}
    for key, value in flat.items():
        name, stat = key.split("__")
        stats.setdefault(name, {})[stat] = value
    return stats


def correlations(fact: DataFrame) -> dict:
    """Step 5: Pearson over all rows, computed in a single scan."""
    pairs = [(a, b) for i, a in enumerate(MEASURES) for b in MEASURES[i + 1:]]
    row = fact.agg(*[F.corr(a, b).alias(f"{a}|{b}") for a, b in pairs]).first().asDict()
    return {k: v for k, v in row.items()}


def weekly_series(fact: DataFrame) -> dict:
    """Step 6: does a store-product series predict itself? Pearson on log1p(demand) per week lag."""
    base = (
        fact.filter(F.pmod(F.col("Cliente_ID"), F.lit(SERIES_SAMPLE_MOD)) == 0)
        .select(*SERIES_KEY, "Semana", F.log1p("Demanda_uni_equil").alias("d"))
    )
    a, b = base.alias("a"), base.alias("b")
    pairs = a.join(b, SERIES_KEY).filter(F.col("b.Semana") > F.col("a.Semana"))
    lags = (
        pairs.groupBy((F.col("b.Semana") - F.col("a.Semana")).alias("lag"))
        .agg(F.count("*").alias("pairs"), F.corr("a.d", "b.d").alias("corr_log1p"))
        .orderBy("lag")
    )
    weeks = base.groupBy(*SERIES_KEY).agg(F.countDistinct("Semana").alias("weeks"))
    return {
        "sample": f"pmod(Cliente_ID, {SERIES_SAMPLE_MOD}) = 0",
        "rows": base.count(),
        "by_lag": [r.asDict() for r in lags.collect()],
        "series_by_weeks_present": {r["weeks"]: r["count"] for r in weeks.groupBy("weeks").count().collect()},
    }


def duplicate_keys(bronze_train: DataFrame) -> dict:
    """Step 1: is the composite key unique? Checked on a 2% client sample (a full shuffle is costly)."""
    sample = bronze_train.filter(F.pmod(F.col("Cliente_ID"), F.lit(SERIES_SAMPLE_MOD)) == 0)
    per_key = sample.groupBy(*ROW_KEY).count()
    row = per_key.agg(F.count("*").alias("keys"), F.sum("count").alias("rows"),
                      F.sum((F.col("count") > 1).cast("long")).alias("keys_with_dupes"),
                      F.max("count").alias("max_rows_per_key")).first().asDict()
    row["sample"] = f"pmod(Cliente_ID, {SERIES_SAMPLE_MOD}) = 0"
    return row


def test_vs_train(spark, fact: DataFrame) -> dict:
    """Step 8 (ML readiness): how much of test (weeks 10-11) was never seen in train?"""
    test = spark.read.parquet(f"{config.BRONZE}/test")
    clients = fact.select("Cliente_ID").distinct()
    products = fact.select("Producto_ID").distinct()
    flagged = (
        test.select("id", "Semana", "Canal_ID", "Cliente_ID", "Producto_ID")
        .join(F.broadcast(clients.withColumn("seen_client", F.lit(1))), "Cliente_ID", "left")
        .join(F.broadcast(products.withColumn("seen_product", F.lit(1))), "Producto_ID", "left")
    )
    row = flagged.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("seen_client").isNull().cast("long")).alias("rows_new_client"),
        F.sum(F.col("seen_product").isNull().cast("long")).alias("rows_new_product"),
        F.countDistinct("Cliente_ID").alias("clients"),
        F.countDistinct("Producto_ID").alias("products"),
    ).first().asDict()
    # Client x product pairs on the 2% client sample: a pair can be new even if both sides are known
    pairs = fact.filter(F.pmod(F.col("Cliente_ID"), F.lit(SERIES_SAMPLE_MOD)) == 0) \
        .select("Cliente_ID", "Producto_ID").distinct().withColumn("seen_pair", F.lit(1))
    t = test.filter(F.pmod(F.col("Cliente_ID"), F.lit(SERIES_SAMPLE_MOD)) == 0)
    p = t.join(pairs, ["Cliente_ID", "Producto_ID"], "left").agg(
        F.count("*").alias("rows"), F.sum(F.col("seen_pair").isNull().cast("long")).alias("rows_new_pair")
    ).first().asDict()
    row["pair_sample"] = p
    row["by_channel"] = {r["Canal_ID"]: r["count"] for r in test.groupBy("Canal_ID").count().collect()}
    row["by_week"] = {r["Semana"]: r["count"] for r in test.groupBy("Semana").count().collect()}
    return row


def top_return_rows(fact: DataFrame) -> DataFrame:
    """Step 7: the largest single rows of returned units, in full, to judge them one by one."""
    return fact.orderBy(F.desc("Dev_uni_proxima")).limit(25).select(
        *ROW_KEY, "State", "product_name", "brand", *MEASURES)


def main() -> None:
    spark = get_spark("bimbo-eda")
    os.makedirs(OUT, exist_ok=True)
    bronze_train = spark.read.parquet(f"{config.BRONZE}/train")
    fact = spark.read.parquet(f"{config.SILVER}/fact_sales")
    save_json("meta", {"application_id": spark.sparkContext.applicationId,
                       "row_sample_fraction": ROW_SAMPLE_FRACTION,
                       "client_sample_mod": CLIENT_SAMPLE_MOD, "series_sample_mod": SERIES_SAMPLE_MOD})

    # 1. Data understanding
    save_json("tables", profile_tables(spark))
    save_json("duplicate_keys", duplicate_keys(bronze_train))
    save_json("fact_schema", [{"column": f.name, "type": f.dataType.simpleString()} for f in fact.schema.fields])

    # 2. Descriptive statistics
    save_json("descriptive", descriptive(fact))

    # 3 + 7. Exact value counts per channel: histograms, ECDF, box stats and IQR fences derive from these
    for col_name in ("Demanda_uni_equil", "Venta_uni_hoy", "Dev_uni_proxima"):
        save_csv(f"values_{col_name}", fact.groupBy("Canal_ID", col_name).count())
    price_bin = F.floor(F.log10("unit_price") * 20)  # 20 bins per decade
    save_csv("unit_price_bins", fact.filter(F.col("unit_price") > 0)
             .groupBy("Canal_ID", price_bin.alias("bin")).count())

    # 4 + 6 + 7. One cube answers the categorical, time and outlier questions
    cube = fact.groupBy("Semana", "Canal_ID", "State", "brand").agg(
        F.count("*").alias("rows"),
        F.sum("Demanda_uni_equil").alias("demand_units"),
        F.sum("Venta_uni_hoy").alias("sales_units"),
        F.sum("Venta_hoy").alias("sales_pesos"),
        F.sum("Dev_uni_proxima").alias("return_units"),
        F.sum("Dev_proxima").alias("return_pesos"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum(F.col("is_outlier").cast("long")).alias("outlier_rows"),
        F.sum(F.when(F.col("is_outlier"), F.col("Demanda_uni_equil")).otherwise(0)).alias("outlier_units"),
        F.sum(F.col("is_zero_demand").cast("long")).alias("zero_demand_rows"),
        F.sum((F.col("Dev_uni_proxima") > 0).cast("long")).alias("rows_with_returns"),
        F.sum(F.col("is_unknown_client").cast("long")).alias("unknown_client_rows"),
    )
    save_csv("cube", cube)
    save_csv("products", fact.groupBy("Producto_ID", "product_name", "brand").agg(
        F.count("*").alias("rows"), F.sum("Demanda_uni_equil").alias("demand_units"),
        F.sum("Venta_uni_hoy").alias("sales_units"), F.sum("Venta_hoy").alias("sales_pesos"),
        F.sum("Dev_uni_proxima").alias("return_units"), F.sum("net_revenue").alias("net_revenue"),
        F.min("unit_price").alias("min_price"), F.max("unit_price").alias("max_price"),
        F.avg("unit_price").alias("avg_price"),
    ))

    # 5. Relationships
    save_json("pearson_all_rows", correlations(fact))
    save_csv("row_sample", fact.sample(False, ROW_SAMPLE_FRACTION, seed=42).select(
        "Canal_ID", *MEASURES, "unit_price", "return_rate", "is_outlier"))

    # 6. Time
    save_json("lag_correlation", weekly_series(fact))

    # 7 + 8. Clients: all of them from gold for concentration, a richer sample for PCA / t-SNE
    segments = spark.read.parquet(f"{config.GOLD}/client_segments")
    save_csv("clients_all", segments.select("net_revenue", "demand_units", "active_weeks", "return_rate", "segment"))
    sampled = fact.filter(F.pmod(F.col("Cliente_ID"), F.lit(CLIENT_SAMPLE_MOD)) == 0)
    features = sampled.groupBy("Cliente_ID").agg(
        F.count("*").alias("rows"),
        F.countDistinct("Producto_ID").alias("n_products"),
        F.countDistinct("Ruta_SAK").alias("n_routes"),
        F.countDistinct("Semana").alias("active_weeks"),
        F.sum("Venta_uni_hoy").alias("sales_units"),
        F.sum("Dev_uni_proxima").alias("return_units"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("Venta_hoy").alias("sales_pesos"),
    )
    main_channel = (
        sampled.groupBy("Cliente_ID", "Canal_ID").count()
        .withColumn("_rn", F.row_number().over(Window.partitionBy("Cliente_ID").orderBy(F.desc("count"), "Canal_ID")))
        .filter("_rn = 1").select("Cliente_ID", F.col("Canal_ID").alias("main_channel"))
    )
    save_csv("client_features", features.join(main_channel, "Cliente_ID")
             .join(segments.select("Cliente_ID", "segment"), "Cliente_ID"))

    save_csv("top_return_rows", top_return_rows(fact))

    # 8. Would these findings hold for forecasting weeks 10-11?
    save_json("test_vs_train", test_vs_train(spark, fact))
    spark.stop()
    log("done")


if __name__ == "__main__":
    main()
