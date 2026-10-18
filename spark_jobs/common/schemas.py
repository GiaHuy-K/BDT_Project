"""Explicit schemas for the raw CSVs (no inferSchema on 3 GB of data)."""
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

TRAIN = StructType([
    StructField("Semana", IntegerType()),
    StructField("Agencia_ID", IntegerType()),
    StructField("Canal_ID", IntegerType()),
    StructField("Ruta_SAK", IntegerType()),
    StructField("Cliente_ID", LongType()),
    StructField("Producto_ID", IntegerType()),
    StructField("Venta_uni_hoy", IntegerType()),
    StructField("Venta_hoy", DoubleType()),
    StructField("Dev_uni_proxima", IntegerType()),
    StructField("Dev_proxima", DoubleType()),
    StructField("Demanda_uni_equil", IntegerType()),
])

TEST = StructType([
    StructField("id", LongType()),
    StructField("Semana", IntegerType()),
    StructField("Agencia_ID", IntegerType()),
    StructField("Canal_ID", IntegerType()),
    StructField("Ruta_SAK", IntegerType()),
    StructField("Cliente_ID", LongType()),
    StructField("Producto_ID", IntegerType()),
])

CLIENTE = StructType([
    StructField("Cliente_ID", LongType()),
    StructField("NombreCliente", StringType()),
])

PRODUCTO = StructType([
    StructField("Producto_ID", IntegerType()),
    StructField("NombreProducto", StringType()),
])

TOWN_STATE = StructType([
    StructField("Agencia_ID", IntegerType()),
    StructField("Town", StringType()),
    StructField("State", StringType()),
])

BY_TABLE = {
    "train": TRAIN,
    "test": TEST,
    "cliente_tabla": CLIENTE,
    "producto_tabla": PRODUCTO,
    "town_state": TOWN_STATE,
}
