"""Forecast: predict weekly demand per delivery row with Spark MLlib, then aggregate for planning.

Validation (week 9, clients the model never trained on) compares GBT with two baselines using
RMSLE, the competition metric. Features for a target week only use weeks <= target - GAP, so the
model never sees the future. Weeks 10-11 (test.csv) have no actuals and are forecast at full scale.

test.csv only lists about a third of a normal week's rows, so its totals are not national demand.
GBT wins per delivery row but the simple baseline wins on state x channel totals, so both are kept:
use GBT for how much to deliver per store and product, the baseline when summing up.

Output:
  gold/forecast_metrics                  - RMSLE and aggregate error per model, on week 9
  gold/forecast_demand_by_state_channel  - predicted units for the rows listed in weeks 10-11
  logs/forecast_report_<applicationId>.json - metrics, feature importances, correction factors
"""
from __future__ import annotations  # the Spark image ships Python 3.8

import json
import os

from pyspark import StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from common import config
from common.spark_session import get_spark

GAP = 2  # week 11 can only use data up to week 9, so train with the same two-week distance
TRAIN_WEEKS = [6, 7, 8]
VALID_WEEK = 9
LAST_KNOWN_WEEK = 9
TRAIN_CLIENT = 0   # pmod(Cliente_ID, 50) groups: 2% of clients each, disjoint, deterministic
VALID_CLIENT = 10
STATS_MOD = 10     # product and agency averages use 10% of clients; both groups above are inside it
LOG_DIR = "/opt/pipeline/logs"
KEY_COLS = ["Agencia_ID", "Canal_ID", "Ruta_SAK", "Cliente_ID", "Producto_ID"]
FEATURES = [
    "pair_mean", "pair_last", "pair_weeks", "has_pair", "client_mean", "client_weeks",
    "route_product_mean", "agency_product_mean", "product_mean", "product_price", "channel_mean",
]


def history_stats(history: DataFrame, pair_history: DataFrame, cutoff: int) -> dict:
    """Averages of log1p(demand) from weeks <= cutoff, at several levels of detail."""
    h = history.filter(F.col("Semana") <= cutoff)
    p = pair_history.filter(F.col("Semana") <= cutoff)
    return {
        "pair": p.groupBy("Cliente_ID", "Producto_ID").agg(
            F.avg("d").alias("pair_mean"), F.max_by("d", "Semana").alias("pair_last"),
            F.countDistinct("Semana").alias("pair_weeks")),
        "client": p.groupBy("Cliente_ID").agg(
            F.avg("d").alias("client_mean"), F.countDistinct("Semana").alias("client_weeks")),
        "route_product": h.groupBy("Ruta_SAK", "Producto_ID").agg(F.avg("d").alias("route_product_mean")),
        "agency_product": h.groupBy("Agencia_ID", "Producto_ID").agg(F.avg("d").alias("agency_product_mean")),
        "product": h.groupBy("Producto_ID").agg(
            F.avg("d").alias("product_mean"), F.avg("unit_price").alias("product_price")),
        "channel": h.groupBy("Canal_ID").agg(F.avg("d").alias("channel_mean")),
        "global": h.agg(F.avg("d")).first()[0],
    }


def add_features(target: DataFrame, s: dict) -> DataFrame:
    df = (
        target.join(s["pair"], ["Cliente_ID", "Producto_ID"], "left")
        .join(s["client"], "Cliente_ID", "left")
        .join(s["route_product"], ["Ruta_SAK", "Producto_ID"], "left")
        .join(s["agency_product"], ["Agencia_ID", "Producto_ID"], "left")
        .join(F.broadcast(s["product"]), "Producto_ID", "left")
        .join(F.broadcast(s["channel"]), "Canal_ID", "left")
    )
    # Baseline: the most specific average that exists for this row
    df = df.withColumn("has_pair", F.col("pair_mean").isNotNull().cast("double")).withColumn(
        "baseline_log",
        F.coalesce("pair_mean", "agency_product_mean", "product_mean", "channel_mean", F.lit(s["global"])),
    ).withColumn(
        "product_baseline_log", F.coalesce("agency_product_mean", "product_mean", "channel_mean", F.lit(s["global"]))
    )
    return df.fillna(-1.0, subset=[c for c in FEATURES if c != "has_pair"])


def rmsle(df: DataFrame, pred_log: str, actual_log: str = "d") -> float:
    return df.agg(F.sqrt(F.avg(F.pow(F.greatest(F.col(pred_log), F.lit(0.0)) - F.col(actual_log), 2)))).first()[0]


def main() -> None:
    spark = get_spark("bimbo-forecast")
    fact = spark.read.parquet(f"{config.SILVER}/fact_sales").select(
        "Semana", *KEY_COLS, "State", "Demanda_uni_equil", "unit_price"
    ).withColumn("d", F.log1p("Demanda_uni_equil"))

    # 1. Training and validation frames, from a sample small enough for 1.5 GB executors
    sample = fact.filter(F.pmod("Cliente_ID", F.lit(STATS_MOD)) == 0).persist(StorageLevel.DISK_ONLY)
    groups = F.pmod("Cliente_ID", F.lit(50))
    train_clients = sample.filter(groups == TRAIN_CLIENT)
    valid_clients = sample.filter(groups == VALID_CLIENT)

    def frame(clients: DataFrame, week: int) -> DataFrame:
        s = history_stats(sample, clients, week - GAP)
        return add_features(clients.filter(F.col("Semana") == week), s)

    train = None
    for week in TRAIN_WEEKS:
        part = frame(train_clients, week)
        train = part if train is None else train.unionByName(part)
    train = train.persist(StorageLevel.DISK_ONLY)
    valid = frame(valid_clients, VALID_WEEK).persist(StorageLevel.DISK_ONLY)
    n_train, n_valid = train.count(), valid.count()
    print(f"[forecast] train rows {n_train:,}, validation rows {n_valid:,}")

    # 2. Model
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features")
    gbt = GBTRegressor(labelCol="d", featuresCol="features", predictionCol="gbt_log",
                       maxDepth=5, maxIter=40, stepSize=0.1, subsamplingRate=0.5, seed=42)
    model = Pipeline(stages=[assembler, gbt]).fit(train)
    scored = model.transform(valid).persist(StorageLevel.DISK_ONLY)
    scored_train = model.transform(train)

    # 3. Row-level accuracy on week 9, unseen clients
    known = scored.filter("has_pair = 1")
    new = scored.filter("has_pair = 0")
    metrics = {
        "valid_rows": n_valid, "train_rows": n_train, "new_pair_share_pct": new.count() / n_valid * 100,
        "rmsle": {
            "product_baseline": rmsle(scored, "product_baseline_log"),
            "history_baseline": rmsle(scored, "baseline_log"),
            "gbt": rmsle(scored, "gbt_log"),
        },
        "rmsle_known_pair": {"history_baseline": rmsle(known, "baseline_log"), "gbt": rmsle(known, "gbt_log")},
        "rmsle_new_pair": {"history_baseline": rmsle(new, "baseline_log"), "gbt": rmsle(new, "gbt_log")},
    }

    # 4. A mean of logs under-predicts totals, so scale by channel, learned on the training weeks only
    factors = {
        r["Canal_ID"]: r["factor"]
        for r in scored_train.groupBy("Canal_ID").agg(
            (F.sum("Demanda_uni_equil") / F.sum(F.expm1(F.greatest("gbt_log", F.lit(0.0))))).alias("factor")
        ).collect()
    }
    mapping = F.create_map(*[x for k, v in factors.items() for x in (F.lit(k), F.lit(float(v)))])

    def units(col_log: str):
        return F.expm1(F.greatest(F.col(col_log), F.lit(0.0)))

    # Aggregate accuracy at the level planners use: state x channel, week 9, these clients
    agg = scored.withColumn("gbt_units", units("gbt_log") * F.coalesce(mapping[F.col("Canal_ID")], F.lit(1.0))).groupBy(
        "State", "Canal_ID"
    ).agg(F.sum("Demanda_uni_equil").alias("actual"), F.sum("gbt_units").alias("gbt"),
          F.sum(units("gbt_log")).alias("gbt_raw"), F.sum(units("baseline_log")).alias("baseline"))
    a = agg.agg(F.sum("actual").alias("actual"), F.sum(F.abs(F.col("gbt") - F.col("actual"))).alias("gbt"),
                F.sum(F.abs(F.col("gbt_raw") - F.col("actual"))).alias("raw"),
                F.sum(F.abs(F.col("baseline") - F.col("actual"))).alias("base")).first()
    metrics["wape_state_channel_pct"] = {"gbt_corrected": a["gbt"] / a["actual"] * 100, "gbt_raw": a["raw"] / a["actual"] * 100,
                                         "history_baseline": a["base"] / a["actual"] * 100}
    metrics["correction_factor_by_channel"] = factors
    importance = model.stages[-1].featureImportances
    metrics["feature_importance"] = {f: float(importance[i]) for i, f in enumerate(FEATURES)}
    print(f"[forecast] RMSLE week 9: {metrics['rmsle']}")
    print(f"[forecast] WAPE state x channel: {metrics['wape_state_channel_pct']}")

    # 5. Weeks 10-11 at full scale. test.csv lists the planned deliveries; history runs up to week 9.
    full = fact.select("Semana", *KEY_COLS, "d", "unit_price")
    stats = history_stats(full, full, LAST_KNOWN_WEEK)
    agencies = spark.read.parquet(f"{config.SILVER}/dim_agencia").select("Agencia_ID", "State")
    test = spark.read.parquet(f"{config.BRONZE}/test").select("Semana", *KEY_COLS)
    predicted = model.transform(add_features(test, stats)).withColumn(
        "gbt_units", units("gbt_log") * F.coalesce(mapping[F.col("Canal_ID")], F.lit(1.0))
    ).withColumn("baseline_units", units("baseline_log"))
    forecast = (
        predicted.join(F.broadcast(agencies), "Agencia_ID", "left")
        .groupBy("Semana", "State", "Canal_ID")
        .agg(F.count("*").alias("listed_rows"), F.round(F.sum("gbt_units"), 0).alias("gbt_units"),
             F.round(F.sum("baseline_units"), 0).alias("baseline_units"))
    )
    forecast.write.mode("overwrite").parquet(f"{config.GOLD}/forecast_demand_by_state_channel")
    written = spark.read.parquet(f"{config.GOLD}/forecast_demand_by_state_channel")
    print(f"[forecast] forecast_demand_by_state_channel: {written.count():,} rows")
    metrics["forecast_by_week"] = [r.asDict() for r in written.groupBy("Semana").agg(
        F.sum("listed_rows").alias("listed_rows"), F.sum("gbt_units").alias("gbt_units"),
        F.sum("baseline_units").alias("baseline_units")).orderBy("Semana").collect()]

    wape = {"gbt": metrics["wape_state_channel_pct"]["gbt_corrected"],
            "history_baseline": metrics["wape_state_channel_pct"]["history_baseline"]}
    rows = [Row(model=m, rmsle=float(v), wape_state_channel_pct=float(wape[m]) if m in wape else None,
                scope="week 9, unseen clients") for m, v in metrics["rmsle"].items()]
    schema = "model string, rmsle double, wape_state_channel_pct double, scope string"
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(f"{config.GOLD}/forecast_metrics")

    metrics["application_id"] = spark.sparkContext.applicationId
    text = json.dumps(metrics, indent=2, default=str)
    print(f"[forecast] report:\n{text}")
    if os.path.isdir(LOG_DIR):
        with open(f"{LOG_DIR}/forecast_report_{metrics['application_id']}.json", "w", encoding="utf-8") as f:
            f.write(text)
    spark.stop()


if __name__ == "__main__":
    main()
