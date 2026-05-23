"""
Gold Layer — gold.py
Builds all analytical (Gold) tables from Silver data.
Tables produced:
  - gold_sales_daily
  - gold_sales_by_channel
  - gold_sales_by_region
  - gold_top_products
  - gold_return_rate
  - gold_customer_basket
  - gold_sales_by_category
  - gold_sales_by_segment
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_session import get_spark_session
from pyspark.sql import functions as F, DataFrame

CATALOG = "lakehouse"
SILVER_DB = "silver"
GOLD_DB = "gold"


def write_gold(spark, df: DataFrame, table_name: str, partition_cols=None):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{GOLD_DB} "
              f"LOCATION 's3a://lakehouse-gold/'")
    df = df.withColumn("_computed_at", F.current_timestamp())
    writer = df.writeTo(f"{CATALOG}.{GOLD_DB}.{table_name}") \
               .tableProperty("write.format.default", "parquet")
    if partition_cols:
        writer = writer.partitionedBy(*partition_cols)
    writer.createOrReplace()
    print(f"[GOLD] ✓ {table_name}: {df.count()} rows")


def build_gold_sales_daily(spark):
    """CA par jour"""
    df = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales") \
              .filter(F.col("statut") != "annulé")
    return df.groupBy("date", "annee", "mois", "semaine").agg(
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.sum("quantite").alias("quantite_totale"),
        F.avg("montant").alias("panier_moyen"),
    ).orderBy("date")


def build_gold_sales_by_channel(spark):
    """CA par canal de vente"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    channels = spark.table(f"{CATALOG}.{SILVER_DB}.silver_channels")
    df = sales.join(channels, "id_canal", "left")
    return df.groupBy("id_canal", "nom", "type").agg(
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.sum("quantite").alias("quantite_totale"),
        F.avg("montant").alias("panier_moyen"),
    ).orderBy(F.desc("ca_total"))


def build_gold_sales_by_region(spark):
    """CA par région / ville"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    customers = spark.table(f"{CATALOG}.{SILVER_DB}.silver_customers")
    df = sales.join(customers.select("id_client", "ville", "region"), "id_client", "left")
    return df.groupBy("region", "ville").agg(
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.countDistinct("id_client").alias("nb_clients"),
    ).orderBy(F.desc("ca_total"))


def build_gold_top_products(spark):
    """Top produits vendus"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    products = spark.table(f"{CATALOG}.{SILVER_DB}.silver_products")
    df = sales.join(products.select("id_produit", "nom_produit", "categorie", "marque"), "id_produit", "left")
    return df.groupBy("id_produit", "nom_produit", "categorie", "marque").agg(
        F.sum("quantite").alias("quantite_vendue"),
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.avg("prix_unitaire").alias("prix_moyen"),
    ).orderBy(F.desc("ca_total"))


def build_gold_return_rate(spark):
    """Taux de retour par produit"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    returns = spark.table(f"{CATALOG}.{SILVER_DB}.silver_returns")
    products = spark.table(f"{CATALOG}.{SILVER_DB}.silver_products")

    sales_agg = sales.groupBy("id_produit").agg(
        F.sum("quantite").alias("quantite_vendue"),
        F.count("id_vente").alias("nb_ventes"),
        F.sum("montant").alias("ca_total"),
    )
    returns_agg = returns.groupBy("id_produit").agg(
        F.sum("quantite_retournee").alias("quantite_retournee"),
        F.count("id_retour").alias("nb_retours"),
        F.sum("montant_rembourse").alias("montant_rembourse"),
    )
    df = sales_agg.join(returns_agg, "id_produit", "left") \
                  .join(products.select("id_produit", "nom_produit", "categorie"), "id_produit", "left")
    df = df.withColumn("quantite_retournee", F.coalesce("quantite_retournee", F.lit(0))) \
           .withColumn("nb_retours", F.coalesce("nb_retours", F.lit(0))) \
           .withColumn("montant_rembourse", F.coalesce("montant_rembourse", F.lit(0.0))) \
           .withColumn("taux_retour_pct",
               F.round(F.col("quantite_retournee") / F.col("quantite_vendue") * 100, 2))
    return df.orderBy(F.desc("taux_retour_pct"))


def build_gold_customer_basket(spark):
    """Panier moyen par client"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    customers = spark.table(f"{CATALOG}.{SILVER_DB}.silver_customers")
    df = sales.groupBy("id_client").agg(
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.avg("montant").alias("panier_moyen"),
        F.sum("quantite").alias("quantite_totale"),
        F.min("date").alias("premiere_vente"),
        F.max("date").alias("derniere_vente"),
    )
    df = df.join(customers.select("id_client", "nom", "prenom", "ville", "region", "segment"), "id_client", "left")
    return df.orderBy(F.desc("ca_total"))


def build_gold_sales_by_category(spark):
    """Évolution des ventes par catégorie et mois"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    products = spark.table(f"{CATALOG}.{SILVER_DB}.silver_products")
    df = sales.join(products.select("id_produit", "categorie"), "id_produit", "left")
    return df.groupBy("categorie", "annee", "mois").agg(
        F.sum("montant").alias("ca_total"),
        F.sum("quantite").alias("quantite_vendue"),
        F.count("id_vente").alias("nb_ventes"),
    ).orderBy("categorie", "annee", "mois")


def build_gold_sales_by_segment(spark):
    """Répartition des ventes par segment de clientèle"""
    sales = spark.table(f"{CATALOG}.{SILVER_DB}.silver_sales").filter(F.col("statut") != "annulé")
    customers = spark.table(f"{CATALOG}.{SILVER_DB}.silver_customers")
    df = sales.join(customers.select("id_client", "segment"), "id_client", "left")
    return df.groupBy("segment").agg(
        F.sum("montant").alias("ca_total"),
        F.count("id_vente").alias("nb_ventes"),
        F.countDistinct("id_client").alias("nb_clients"),
        F.avg("montant").alias("panier_moyen"),
    ).orderBy(F.desc("ca_total"))


def main():
    spark = get_spark_session("Gold-Analytics")
    write_gold(spark, build_gold_sales_daily(spark),       "gold_sales_daily",       ["annee", "mois"])
    write_gold(spark, build_gold_sales_by_channel(spark),  "gold_sales_by_channel")
    write_gold(spark, build_gold_sales_by_region(spark),   "gold_sales_by_region")
    write_gold(spark, build_gold_top_products(spark),      "gold_top_products")
    write_gold(spark, build_gold_return_rate(spark),       "gold_return_rate")
    write_gold(spark, build_gold_customer_basket(spark),   "gold_customer_basket")
    write_gold(spark, build_gold_sales_by_category(spark), "gold_sales_by_category", ["annee", "mois"])
    write_gold(spark, build_gold_sales_by_segment(spark),  "gold_sales_by_segment")
    print("[GOLD] All tables computed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
