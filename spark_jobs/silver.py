"""Silver: cleaned, deduplicated, enriched data.

Output tables (s3a://processed/silver/...):
  dim_cliente   - one row per Cliente_ID, normalized name
  dim_producto  - name split into short name, pieces, weight_g, brand
  dim_agencia   - Agencia_ID -> town, normalized state
  fact_sales    - train joined with the 3 dims, + return_rate, net_revenue, quality flags
  rejected      - rows failing quality rules, with a reason column
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import config
from common.spark_session import get_spark


def clean_clientes(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"{config.BRONZE}/cliente_tabla")
    # TODO: trim/upper NombreCliente, label "SIN NOMBRE" as unknown,
    # dedup the 4,862 duplicated Cliente_ID (keep one name per ID).
    return df.dropDuplicates(["Cliente_ID"])


def clean_productos(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"{config.BRONZE}/producto_tabla")
    # TODO: regex on NombreProducto, e.g. "Bimbollos Ext sAjonjoli 6p 480g BIM 41"
    #   pieces   <- (\d+)p
    #   weight_g <- (\d+)(g|kg|ml)
    #   brand    <- the token before the trailing Producto_ID
    return df


def clean_agencias(spark: SparkSession) -> DataFrame:
    df = spark.read.parquet(f"{config.BRONZE}/town_state")
    # TODO: normalize State spelling/case (e.g. "MÉXICO, D.F." vs "Queretaro de Arteaga").
    return df


def build_fact(spark: SparkSession, cli: DataFrame, prod: DataFrame, ag: DataFrame) -> DataFrame:
    train = spark.read.parquet(f"{config.BRONZE}/train")
    # TODO: quality rules -> split valid rows from `rejected` (negative values, returns > sales, ...)
    # TODO: derive return_rate, net_revenue = Venta_hoy - Dev_proxima, outlier flag (p99.9)
    fact = (
        train.join(F.broadcast(cli), "Cliente_ID", "left")
        .join(F.broadcast(prod), "Producto_ID", "left")
        .join(F.broadcast(ag), "Agencia_ID", "left")
    )
    # TODO: assert fact.count() == train.count() (duplicate dim keys would multiply rows)
    return fact


def main() -> None:
    spark = get_spark("bimbo-silver")
    cli, prod, ag = clean_clientes(spark), clean_productos(spark), clean_agencias(spark)
    for name, df in {"dim_cliente": cli, "dim_producto": prod, "dim_agencia": ag}.items():
        df.write.mode("overwrite").parquet(f"{config.SILVER}/{name}")
    build_fact(spark, cli, prod, ag).write.mode("overwrite").partitionBy("Semana") \
        .parquet(f"{config.SILVER}/fact_sales")
    spark.stop()


if __name__ == "__main__":
    main()
