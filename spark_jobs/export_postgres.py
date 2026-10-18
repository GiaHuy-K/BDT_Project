"""Serving: copy gold Parquet tables into PostgreSQL schema `gold` for Metabase."""
from pyspark.sql.utils import AnalysisException

from common import config
from common.spark_session import get_spark


def main() -> None:
    spark = get_spark("bimbo-export-postgres")
    for name in config.GOLD_TABLES:
        try:
            df = spark.read.parquet(f"{config.GOLD}/{name}")
        except AnalysisException:
            print(f"[export] {name}: no gold output yet, skipped")
            continue
        df.write.mode("overwrite").option("truncate", "true") \
            .jdbc(config.JDBC_URL, f"gold.{name}", properties=config.JDBC_PROPS)
        print(f"[export] gold.{name}: {df.count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
