"""Bronze: raw CSV in MinIO -> typed Parquet, no business logic.

Adds lineage columns and partitions the fact tables by week (Semana).
"""
from pyspark.sql import functions as F

from common import config, schemas
from common.spark_session import get_spark


def main() -> None:
    spark = get_spark("bimbo-bronze")
    for table in config.TABLES:
        src = f"{config.RAW}/{table}/{table}.csv"
        df = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .schema(schemas.BY_TABLE[table])
            .csv(src)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_ingested_at", F.current_timestamp())
        )
        writer = df.write.mode("overwrite")
        if "Semana" in df.columns:
            writer = writer.partitionBy("Semana")
        writer.parquet(f"{config.BRONZE}/{table}")
        print(f"[bronze] {table}: {spark.read.parquet(f'{config.BRONZE}/{table}').count():,} rows")
    spark.stop()


if __name__ == "__main__":
    main()
