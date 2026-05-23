"""
Bronze Layer — ingest_csv.py
Reads raw CSV files from MinIO (lakehouse-raw bucket) and writes them
as Iceberg tables in the Bronze layer (lakehouse-bronze bucket).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F

RAW_BASE = "s3a://lakehouse-bronze/raw/csv"
BRONZE_CATALOG = "lakehouse"
BRONZE_DB = "bronze"

TABLES = {
    "bronze_sales":     f"{RAW_BASE}/sales.csv",
    "bronze_customers": f"{RAW_BASE}/customers.csv",
    "bronze_products":  f"{RAW_BASE}/products.csv",
    "bronze_stocks":    f"{RAW_BASE}/stocks.csv",
    "bronze_returns":   f"{RAW_BASE}/returns.csv",
    "bronze_channels":  f"{RAW_BASE}/channels.csv",
}


def ingest_csv_to_bronze(spark, table_name: str, csv_path: str):
    print(f"[BRONZE] Ingesting {csv_path} → {BRONZE_DB}.{table_name}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(csv_path)
    )
    # Add ingestion metadata
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.lit(csv_path))

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_CATALOG}.{BRONZE_DB} "
              f"LOCATION 's3a://lakehouse-bronze/'")

    (
        df.writeTo(f"{BRONZE_CATALOG}.{BRONZE_DB}.{table_name}")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.metadata.compression-codec", "gzip")
        .createOrReplace()
    )
    print(f"[BRONZE] ✓ {table_name}: {df.count()} rows written")


def main():
    spark = get_spark_session("Bronze-CSV-Ingestion")
    for table_name, csv_path in TABLES.items():
        ingest_csv_to_bronze(spark, table_name, csv_path)
    print("[BRONZE] All tables ingested successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
