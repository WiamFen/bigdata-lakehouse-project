"""
Silver Layer — silver.py
Reads Bronze Iceberg tables, cleans/normalizes them, and writes Silver tables.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.types import DoubleType, IntegerType, DateType

CATALOG = "lakehouse"
BRONZE_DB = "bronze"
SILVER_DB = "silver"


def write_silver(spark, df: DataFrame, table_name: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SILVER_DB} "
              f"LOCATION 's3a://lakehouse-silver/'")
    df = df.withColumn("_updated_at", F.current_timestamp())
    (
        df.writeTo(f"{CATALOG}.{SILVER_DB}.{table_name}")
        .tableProperty("write.format.default", "parquet")
        .createOrReplace()
    )
    print(f"[SILVER] ✓ {table_name}: {df.count()} rows")


def clean_sales(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_sales")
    df = df.dropDuplicates(["id_vente"])
    df = df.filter(F.col("id_vente").isNotNull() & F.col("id_client").isNotNull())
    df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))
    df = df.withColumn("quantite", F.col("quantite").cast(IntegerType()))
    df = df.withColumn("prix_unitaire", F.col("prix_unitaire").cast(DoubleType()))
    df = df.withColumn("remise_pct", F.coalesce(F.col("remise_pct").cast(DoubleType()), F.lit(0.0)))
    df = df.withColumn("montant", F.col("montant").cast(DoubleType()))
    df = df.withColumn("statut", F.lower(F.trim(F.col("statut"))))
    df = df.withColumn("annee", F.year("date"))
    df = df.withColumn("mois", F.month("date"))
    df = df.withColumn("semaine", F.weekofyear("date"))
    df = df.drop("_ingested_at", "_source_file")
    return df


def clean_customers(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_customers")
    df = df.dropDuplicates(["id_client"])
    df = df.filter(F.col("id_client").isNotNull())
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    df = df.withColumn("ville", F.initcap(F.trim(F.col("ville"))))
    df = df.withColumn("region", F.trim(F.col("region")))
    df = df.withColumn("segment", F.trim(F.col("segment")))
    df = df.withColumn("date_inscription", F.to_date("date_inscription", "yyyy-MM-dd"))
    df = df.withColumn("age", F.col("age").cast(IntegerType()))
    df = df.withColumn("telephone", F.regexp_replace("telephone", r"\s+", ""))
    df = df.drop("_ingested_at", "_source_file")
    return df


def clean_products(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_products")
    df = df.dropDuplicates(["id_produit"])
    df = df.filter(F.col("id_produit").isNotNull())
    df = df.withColumn("prix", F.col("prix").cast(DoubleType()))
    df = df.withColumn("prix_achat", F.col("prix_achat").cast(DoubleType()))
    df = df.withColumn("poids_kg", F.col("poids_kg").cast(DoubleType()))
    df = df.withColumn("stock_initial", F.col("stock_initial").cast(IntegerType()))
    df = df.withColumn("categorie", F.trim(F.col("categorie")))
    df = df.withColumn("marque", F.trim(F.col("marque")))
    df = df.withColumn("marge_pct",
        F.round((F.col("prix") - F.col("prix_achat")) / F.col("prix") * 100, 2))
    df = df.drop("_ingested_at", "_source_file")
    return df


def clean_stocks(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_stocks")
    df = df.dropDuplicates(["id_produit", "depot"])
    df = df.withColumn("quantite_disponible", F.col("quantite_disponible").cast(IntegerType()))
    df = df.drop("_ingested_at", "_source_file")
    return df


def clean_returns(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_returns")
    df = df.dropDuplicates(["id_retour"])
    df = df.filter(F.col("id_retour").isNotNull())
    df = df.withColumn("date_retour", F.to_date("date_retour", "yyyy-MM-dd"))
    df = df.withColumn("quantite_retournee", F.col("quantite_retournee").cast(IntegerType()))
    df = df.withColumn("montant_rembourse", F.col("montant_rembourse").cast(DoubleType()))
    df = df.withColumn("motif", F.trim(F.col("motif")))
    df = df.drop("_ingested_at", "_source_file")
    return df


def clean_channels(spark) -> DataFrame:
    df = spark.table(f"{CATALOG}.{BRONZE_DB}.bronze_channels")
    df = df.dropDuplicates(["id_canal"])
    df = df.withColumn("type", F.lower(F.trim(F.col("type"))))
    df = df.withColumn("nom", F.trim(F.col("nom")))
    df = df.drop("_ingested_at", "_source_file")
    return df


def main():
    spark = get_spark_session("Silver-Cleaning")
    write_silver(spark, clean_sales(spark),     "silver_sales")
    write_silver(spark, clean_customers(spark), "silver_customers")
    write_silver(spark, clean_products(spark),  "silver_products")
    write_silver(spark, clean_stocks(spark),    "silver_stocks")
    write_silver(spark, clean_returns(spark),   "silver_returns")
    write_silver(spark, clean_channels(spark),  "silver_channels")
    print("[SILVER] All tables cleaned and written.")
    spark.stop()


if __name__ == "__main__":
    main()
