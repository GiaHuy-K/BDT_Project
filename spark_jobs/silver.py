"""Silver: cleaned, deduplicated, enriched data.

Output tables (s3a://processed/silver/...):
  dim_cliente   - one row per Cliente_ID, normalized name, unknown flag
  dim_producto  - name split into short name, pieces, weight_g, brand
  dim_agencia   - Agencia_ID -> town, normalized state
  fact_sales    - valid train rows joined with the 3 dims, + net_revenue, return_rate, quality flags
  rejected      - train rows failing quality rules, with a `reason` column

Invariant checked at the end: fact_sales + rejected == bronze train (no row lost or duplicated).
A quality report (row counts per rule, dim stats) is printed and written to logs/.
"""
from __future__ import annotations  # the Spark image ships Python 3.8

import json
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import config
from common.spark_session import get_spark

KEYS = ["Semana", "Agencia_ID", "Canal_ID", "Ruta_SAK", "Cliente_ID", "Producto_ID"]
MEASURES = ["Venta_uni_hoy", "Venta_hoy", "Dev_uni_proxima", "Dev_proxima", "Demanda_uni_equil"]
UNKNOWN_NAMES = ["SIN NOMBRE", "NO IDENTIFICADO", "NO IDENTIFICADO 0", ""]
OUTLIER_QUANTILE = 0.999
LOG_DIR = "/opt/pipeline/logs"

# Spelling variants left after upper-casing and removing accents
STATE_ALIASES = {
    "QUERETARO DE ARTEAGA": "QUERETARO",
    "MEXICO D.F.": "MEXICO, D.F.",
    "DISTRITO FEDERAL": "MEXICO, D.F.",
}


def normalize_text(col):
    """Upper-case, strip accents, collapse whitespace."""
    col = F.translate(F.upper(col), "ÁÉÍÓÚÜÑ", "AEIOUUN")
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def check_unique(df: DataFrame, key: str, name: str) -> None:
    """A duplicated dim key would multiply fact rows in the join."""
    dupes = df.groupBy(key).count().filter("count > 1").count()
    if dupes:
        raise AssertionError(f"{name}: {dupes} duplicated {key} values")


def clean_clientes(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"{config.BRONZE}/cliente_tabla").withColumn(
        "client_name", normalize_text(F.col("NombreCliente"))
    )
    df = df.withColumn(
        "is_unknown_client",
        F.col("client_name").isNull() | F.col("client_name").isin(UNKNOWN_NAMES),
    ).withColumn(
        "client_name", F.when(F.col("is_unknown_client"), F.lit("UNKNOWN")).otherwise(F.col("client_name"))
    )
    # Duplicated IDs: keep a real name over UNKNOWN, then the longest (most complete) one.
    w = Window.partitionBy("Cliente_ID").orderBy(
        F.col("is_unknown_client").asc(), F.length("client_name").desc(), F.col("client_name")
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .select("Cliente_ID", "client_name", "is_unknown_client")
    )


def clean_productos(spark: SparkSession) -> DataFrame:
    # e.g. "Bimbollos Ext sAjonjoli 6p 480g BIM 41" -> short_name, 6 pieces, 480 g, brand BIM
    df = spark.read.parquet(f"{config.BRONZE}/producto_tabla")
    name = F.col("NombreProducto")
    weight_val = F.regexp_extract(name, r"(?i)\b(\d+(?:\.\d+)?)\s?(kg|g|ml)\b", 1)
    weight_unit = F.lower(F.regexp_extract(name, r"(?i)\b(\d+(?:\.\d+)?)\s?(kg|g|ml)\b", 2))
    pieces = F.regexp_extract(name, r"(?i)\b(\d+)p\b", 1)
    brand = F.regexp_extract(name, r"\s(\S+)\s+\d+\s*$", 1)
    unknown = F.col("Producto_ID") == 0
    return (
        df.dropDuplicates(["Producto_ID"])
        .select(
            "Producto_ID",
            F.trim(name).alias("product_name"),
            F.when(unknown, F.lit("UNKNOWN"))
            .otherwise(normalize_text(F.regexp_extract(name, r"^(\D*)", 1)))
            .alias("product_short_name"),
            F.when(pieces != "", pieces.cast("int")).alias("pieces"),
            F.when(weight_val == "", None)
            .when(weight_unit == "kg", weight_val.cast("double") * 1000)
            .otherwise(weight_val.cast("double"))
            .alias("weight_g"),
            F.when(unknown | (brand == ""), F.lit("UNKNOWN")).otherwise(F.upper(brand)).alias("brand"),
        )
    )


def clean_agencias(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"{config.BRONZE}/town_state")
    state = normalize_text(F.col("State"))
    aliases = F.create_map(*[F.lit(x) for kv in STATE_ALIASES.items() for x in kv])
    return df.dropDuplicates(["Agencia_ID"]).select(
        "Agencia_ID",
        F.trim("Town").alias("Town"),
        F.coalesce(aliases[state], state).alias("State"),
    )


def build_fact(
    spark: SparkSession, cli: DataFrame, prod: DataFrame, ag: DataFrame
) -> tuple[DataFrame, DataFrame, float]:
    train = spark.read.parquet(f"{config.BRONZE}/train").drop("_source_file")

    # Demand is heavily skewed (mean ~7, max 5,000): flag the top 0.1% instead of dropping it.
    threshold = train.filter(F.col("Demanda_uni_equil") >= 0).approxQuantile(
        "Demanda_uni_equil", [OUTLIER_QUANTILE], 0.0001
    )[0]

    joined = (
        train.join(F.broadcast(cli), "Cliente_ID", "left")
        .join(F.broadcast(prod), "Producto_ID", "left")
        .join(F.broadcast(ag), "Agencia_ID", "left")
    )
    # First failing rule wins; order = most severe first.
    reason = (
        F.when(F.greatest(*[F.col(k).isNull().cast("int") for k in KEYS]) == 1, "null_key")
        .when(F.greatest(*[F.col(m).isNull().cast("int") for m in MEASURES]) == 1, "null_measure")
        .when(F.least(*[F.col(m) for m in MEASURES]) < 0, "negative_value")
        .when((F.col("Venta_uni_hoy") > 0) & (F.col("Venta_hoy") == 0), "sales_without_revenue")
        .when(F.col("State").isNull(), "unknown_agency")
        .when(F.col("brand").isNull(), "unknown_product")
        .when(F.col("client_name").isNull(), "unknown_client")
    )
    joined = joined.withColumn("reason", reason)

    rejected = joined.filter(F.col("reason").isNotNull()).select(*train.columns, "reason")
    fact = (
        joined.filter(F.col("reason").isNull())
        .drop("reason")
        .withColumn("net_revenue", F.round(F.col("Venta_hoy") - F.col("Dev_proxima"), 2))
        .withColumn(
            "return_rate",
            F.when(F.col("Venta_uni_hoy") > 0, F.col("Dev_uni_proxima") / F.col("Venta_uni_hoy")),
        )
        .withColumn(
            "unit_price",
            F.when(F.col("Venta_uni_hoy") > 0, F.round(F.col("Venta_hoy") / F.col("Venta_uni_hoy"), 2)),
        )
        .withColumn("is_outlier", F.col("Demanda_uni_equil") > F.lit(threshold))
        .withColumn("is_zero_demand", F.col("Demanda_uni_equil") == 0)
        # Returns relate to earlier deliveries, so they may exceed this week's sales: flag, keep.
        .withColumn("returns_exceed_sales", F.col("Dev_uni_proxima") > F.col("Venta_uni_hoy"))
        .withColumn(
            "demand_mismatch",
            F.col("Demanda_uni_equil")
            != F.greatest(F.col("Venta_uni_hoy") - F.col("Dev_uni_proxima"), F.lit(0)),
        )
    )
    return fact, rejected, threshold


def quality_report(spark: SparkSession, threshold: float) -> dict:
    """Counts read back from the written Parquet, so they describe what is actually stored."""
    read = lambda t: spark.read.parquet(f"{config.SILVER}/{t}")
    bronze = lambda t: spark.read.parquet(f"{config.BRONZE}/{t}")
    fact, rejected = read("fact_sales"), read("rejected")

    flags = fact.agg(
        F.count("*").alias("rows"),
        *[F.sum(F.col(c).cast("int")).alias(c) for c in
          ["is_outlier", "is_zero_demand", "returns_exceed_sales", "demand_mismatch", "is_unknown_client"]],
        F.round(F.sum("net_revenue"), 2).alias("net_revenue_total"),
    ).first().asDict()
    prod = read("dim_producto")
    prod_stats = prod.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("pieces").isNotNull().cast("int")).alias("with_pieces"),
        F.sum(F.col("weight_g").isNotNull().cast("int")).alias("with_weight"),
        F.sum((F.col("brand") != "UNKNOWN").cast("int")).alias("with_brand"),
        F.countDistinct("brand").alias("distinct_brands"),
    ).first().asDict()

    report = {
        "application_id": spark.sparkContext.applicationId,
        "bronze_train_rows": bronze("train").count(),
        "fact_rows": flags.pop("rows"),
        "rejected_rows": rejected.count(),
        "rejected_by_reason": {r["reason"]: r["count"] for r in rejected.groupBy("reason").count().collect()},
        "fact_flags": flags,
        "outlier_threshold_demand": threshold,
        "fact_rows_by_week": {r["Semana"]: r["count"] for r in fact.groupBy("Semana").count().orderBy("Semana").collect()},
        "dim_cliente": {
            "bronze_rows": bronze("cliente_tabla").count(),
            "bronze_distinct_ids": bronze("cliente_tabla").select("Cliente_ID").distinct().count(),
            "silver_rows": read("dim_cliente").count(),
            "unknown_clients": read("dim_cliente").filter("is_unknown_client").count(),
        },
        "dim_producto": prod_stats,
        "dim_agencia": {
            "rows": read("dim_agencia").count(),
            "states_before": bronze("town_state").select("State").distinct().count(),
            "states_after": read("dim_agencia").select("State").distinct().count(),
        },
    }
    report["invariant_ok"] = report["fact_rows"] + report["rejected_rows"] == report["bronze_train_rows"]
    return report


def main() -> None:
    spark = get_spark("bimbo-silver")
    cli, prod, ag = clean_clientes(spark), clean_productos(spark), clean_agencias(spark)
    for name, df, key in [("dim_cliente", cli, "Cliente_ID"), ("dim_producto", prod, "Producto_ID"),
                          ("dim_agencia", ag, "Agencia_ID")]:
        df.write.mode("overwrite").parquet(f"{config.SILVER}/{name}")
        check_unique(spark.read.parquet(f"{config.SILVER}/{name}"), key, name)
        print(f"[silver] {name}: written, {key} unique")

    # Join against the written dims so fact and dims are guaranteed consistent.
    dims = [spark.read.parquet(f"{config.SILVER}/{n}") for n in ("dim_cliente", "dim_producto", "dim_agencia")]
    fact, rejected, threshold = build_fact(spark, *dims)
    fact.write.mode("overwrite").partitionBy("Semana").parquet(f"{config.SILVER}/fact_sales")
    rejected.write.mode("overwrite").parquet(f"{config.SILVER}/rejected")

    report = quality_report(spark, threshold)
    text = json.dumps(report, indent=2, default=str)
    print(f"[silver] quality report:\n{text}")
    if os.path.isdir(LOG_DIR):
        path = f"{LOG_DIR}/silver_quality_{report['application_id']}.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"[silver] report written to {path}")
    spark.stop()
    if not report["invariant_ok"]:
        raise SystemExit("[silver] FAILED: fact_sales + rejected != bronze train")


if __name__ == "__main__":
    main()
