"""
=============================================================
SILVER — normalize.py
=============================================================
Normalisation et enrichissement des tables Silver :
  - Standardisation des segments clients
  - Calcul des marges produits
  - Enrichissement des ventes avec canal/région

Usage :
  spark-submit /opt/spark-jobs/silver/normalize.py
=============================================================
"""

import sys
sys.path.insert(0, "/opt/spark-jobs")

from pyspark.sql import functions as F
from utils.spark_session import get_spark_session
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("silver_normalize")


# ─────────────────────────────────────────────────────────────
# Normalize Customers
# ─────────────────────────────────────────────────────────────
def normalize_customers(spark):

    log.info("Normalize → silver_customers")

    df = spark.table("lakehouse.silver.silver_customers")

    df = df.withColumn(
        "segment",
        F.when(F.lower(F.col("segment")) == "premium", "Premium")
         .when(F.lower(F.col("segment")) == "standard", "Standard")
         .when(F.lower(F.col("segment")) == "vip", "VIP")
         .otherwise(F.col("segment"))
    )

    df.writeTo(
        "lakehouse.silver.silver_customers"
    ).createOrReplace()

    log.info("✓ Segments normalisés")


# ─────────────────────────────────────────────────────────────
# Enrich Products
# ─────────────────────────────────────────────────────────────
def enrich_products(spark):

    log.info("Normalize → silver_products")

    df = spark.table("lakehouse.silver.silver_products")

    df = (
        df
        .withColumn(
            "marge",
            F.round(F.col("prix") - F.col("prix_achat"), 2)
        )
        .withColumn(
            "taux_marge_pct",
            F.when(
                F.col("prix") > 0,
                F.round(
                    (F.col("marge") / F.col("prix")) * 100,
                    2
                )
            ).otherwise(None)
        )
    )

    df.writeTo(
        "lakehouse.silver.silver_products"
    ).createOrReplace()

    log.info("✓ Produits enrichis")


# ─────────────────────────────────────────────────────────────
# Enrich Sales
# ─────────────────────────────────────────────────────────────
def enrich_sales(spark):

    log.info("Normalize → silver_sales_enriched")

    sales = spark.table("lakehouse.silver.silver_sales")

    channels = (
        spark.table("lakehouse.silver.silver_channels")
        .select(
            "id_canal",
            "type",
            F.col("nom").alias("canal_nom"),
            F.col("ville").alias("canal_ville"),
            F.col("region").alias("canal_region")
        )
    )

    df = sales.join(channels, on="id_canal", how="left")

    df.writeTo(
        "lakehouse.silver.silver_sales_enriched"
    ).createOrReplace()

    log.info("✓ Ventes enrichies")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():

    spark = get_spark_session("Silver_Normalize")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 55)
    log.info("DÉMARRAGE — Normalisation Silver")
    log.info("=" * 55)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

    normalize_customers(spark)
    enrich_products(spark)
    enrich_sales(spark)

    log.info("=" * 55)
    log.info("TERMINÉ — Silver Normalize")

    spark.stop()


if __name__ == "__main__":
    main()