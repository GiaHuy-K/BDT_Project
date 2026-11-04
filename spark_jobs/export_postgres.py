"""Serving: copy gold Parquet tables into PostgreSQL schema `gold` for Metabase."""
from pyspark.sql.utils import AnalysisException

from common import config
from common.spark_session import get_spark

# Columns the analysis queries in sql/analysis/ filter or sort on
INDEXES = {
    "weekly_demand_by_state_channel": ['"State", "Canal_ID", "Semana"'],
    "return_rate_by_product_agency_route": ["is_anomaly, return_rate DESC"],
    "product_ranking_by_state": ['"State", rank'],
    "client_segments": ["segment"],
    "forecast_demand_by_state_channel": ['"State", "Canal_ID", "Semana"'],
}


def create_indexes(spark, name: str) -> None:
    """Run CREATE INDEX over the same JDBC driver Spark uses (no extra Python dependency)."""
    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(
        config.JDBC_URL, config.JDBC_PROPS["user"], config.JDBC_PROPS["password"]
    )
    try:
        stmt = conn.createStatement()
        for i, cols in enumerate(INDEXES.get(name, [])):
            stmt.execute(f"CREATE INDEX IF NOT EXISTS {name}_idx{i} ON gold.{name} ({cols})")
    finally:
        conn.close()


def main() -> None:
    spark = get_spark("bimbo-export-postgres")
    for name in config.GOLD_TABLES:
        try:
            df = spark.read.parquet(f"{config.GOLD}/{name}")
        except AnalysisException:
            print(f"[export] {name}: no gold output yet, skipped")
            continue
        # Plain overwrite drops and recreates the table, so it always matches the gold schema.
        # (truncate=true kept the old table definition and failed once gold.py added columns.)
        df.write.mode("overwrite").jdbc(config.JDBC_URL, f"gold.{name}", properties=config.JDBC_PROPS)
        create_indexes(spark, name)
        print(f"[export] gold.{name}: {df.count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
