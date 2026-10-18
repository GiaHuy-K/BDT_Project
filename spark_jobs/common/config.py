"""Storage paths and connection settings shared by all Spark jobs."""
import os

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "processed")

RAW = f"s3a://{RAW_BUCKET}/grupo_bimbo"
BRONZE = f"s3a://{PROCESSED_BUCKET}/bronze"
SILVER = f"s3a://{PROCESSED_BUCKET}/silver"
GOLD = f"s3a://{PROCESSED_BUCKET}/gold"

TABLES = ["train", "test", "cliente_tabla", "producto_tabla", "town_state"]

# Gold tables exported to PostgreSQL (schema "gold"), one per business question
GOLD_TABLES = [
    "weekly_demand_by_state_channel",       # Q1
    "return_rate_by_product_agency_route",  # Q2
    "product_ranking_by_state",             # Q3
    "client_segments",                      # Q4
]

JDBC_URL = (
    f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/"
    f"{os.getenv('POSTGRES_DB', 'bimbo')}"
)
JDBC_PROPS = {
    "user": os.getenv("POSTGRES_USER", "bimbo"),
    "password": os.getenv("POSTGRES_PASSWORD", "bimbo123"),
    "driver": "org.postgresql.Driver",
}
