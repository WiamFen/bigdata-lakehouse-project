"""
=============================================================
GOLD — gold.py
=============================================================
Construit toutes les tables Gold (KPIs) à partir de Silver.
Colonnes adaptées aux fichiers CSV réels du projet.

Tables produites :
  gold_sales_daily        — CA par jour + canal
  gold_sales_by_channel   — CA par canal de vente
  gold_sales_by_region    — CA par ville/région
  gold_top_products       — Top produits par CA et quantité
  gold_customer_basket    — Panier moyen par client
  gold_return_rate        — Taux de retour par produit
  gold_sales_by_category  — Évolution par catégorie
  gold_customer_segments  — Répartition par segment

Usage :
  spark-submit /opt/spark-jobs/gold/gold.py
=============================================================
"""

import sys
sys.path.insert(0, '/opt/spark-jobs')

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.spark_session import get_spark_session
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("gold")


def ensure_gold(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")


# ─────────────────────────────────────────────────────────────
# GOLD 1 — Ventes quotidiennes
# ─────────────────────────────────────────────────────────────
def gold_sales_daily(spark):
    log.info("Gold → gold_sales_daily")

    # Jointure sales ← channels pour récupérer type du canal
    s  = spark.table("lakehouse.silver.silver_sales")
    ch = spark.table("lakehouse.silver.silver_channels") \
              .select("id_canal", "type", F.col("nom").alias("canal_nom"))

    g = (
        s.join(ch, on="id_canal", how="left")
        .groupBy("date", "annee", "mois", "semaine", "trimestre",
                 "id_canal", "type", "canal_nom")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.sum("quantite").alias("total_quantite"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.countDistinct("id_client").alias("clients_uniques"),
            F.round(F.sum(F.col("montant") * F.coalesce(F.col("remise_pct"), F.lit(0)) / 100), 2)
              .alias("total_remises"),
        )
        .orderBy("date")
    )

    g.writeTo("lakehouse.gold.gold_sales_daily") \
     .partitionedBy("annee", "mois") \
     .createOrReplace()
    log.info(f"  ✓ gold_sales_daily ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 2 — Ventes par canal
# ─────────────────────────────────────────────────────────────
def gold_sales_by_channel(spark):
    log.info("Gold → gold_sales_by_channel")

    s  = spark.table("lakehouse.silver.silver_sales")
    ch = spark.table("lakehouse.silver.silver_channels") \
              .select("id_canal", "type", F.col("nom").alias("canal_nom"))

    g = (
        s.join(ch, on="id_canal", how="left")
        .groupBy("id_canal", "type", "canal_nom", "annee", "mois")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.sum("quantite").alias("total_quantite"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.countDistinct("id_client").alias("clients_uniques"),
        )
    )

    # Part du canal dans le CA total mensuel
    w = Window.partitionBy("annee", "mois")
    # g = g.withColumn("part_ca_pct",
    #     F.round(F.col("chiffre_affaires") /
    #             F.sum("chiffre_affaires").over(w) * 100, 2))

    g = g.withColumn(
    "part_ca_pct",
    F.when(
        F.sum("chiffre_affaires").over(w) != 0,
        F.round(
            F.col("chiffre_affaires") /
            F.sum("chiffre_affaires").over(w) * 100,
            2
        )
    ).otherwise(0)
)            

    g.writeTo("lakehouse.gold.gold_sales_by_channel").createOrReplace()
    log.info(f"  ✓ gold_sales_by_channel ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 3 — Ventes par région/ville (via clients)
# ─────────────────────────────────────────────────────────────
def gold_sales_by_region(spark):
    log.info("Gold → gold_sales_by_region")

    s  = spark.table("lakehouse.silver.silver_sales")
    cu = spark.table("lakehouse.silver.silver_customers") \
              .select("id_client", "ville", "region", "segment")

    g = (
        s.join(cu, on="id_client", how="left")
        .groupBy("ville", "region", "segment", "annee", "mois")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.sum("quantite").alias("total_quantite"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.countDistinct("id_client").alias("clients_uniques"),
        )
    )

    g.writeTo("lakehouse.gold.gold_sales_by_region") \
     .partitionedBy("annee") \
     .createOrReplace()
    log.info(f"  ✓ gold_sales_by_region ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 4 — Top produits
# ─────────────────────────────────────────────────────────────
def gold_top_products(spark):
    log.info("Gold → gold_top_products")

    s  = spark.table("lakehouse.silver.silver_sales")
    pr = spark.table("lakehouse.silver.silver_products") \
              .select("id_produit", "nom_produit", "categorie",
                      "marque", "tranche_prix")

    g = (
        s.join(pr, on="id_produit", how="left")
        .groupBy("id_produit", "nom_produit", "categorie",
                 "marque", "tranche_prix", "annee", "mois")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.sum("quantite").alias("total_quantite_vendue"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
        )
    )

    # Ranking mensuel
    w_ca  = Window.partitionBy("annee", "mois").orderBy(F.desc("chiffre_affaires"))
    w_qty = Window.partitionBy("annee", "mois").orderBy(F.desc("total_quantite_vendue"))
    g = g.withColumn("rang_ca",       F.rank().over(w_ca))
    g = g.withColumn("rang_quantite", F.rank().over(w_qty))

    g.writeTo("lakehouse.gold.gold_top_products") \
     .partitionedBy("annee", "mois") \
     .createOrReplace()
    log.info(f"  ✓ gold_top_products ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 5 — Panier client (RFM simplifié)
# ─────────────────────────────────────────────────────────────
def gold_customer_basket(spark):
    log.info("Gold → gold_customer_basket")

    s  = spark.table("lakehouse.silver.silver_sales")
    cu = spark.table("lakehouse.silver.silver_customers") \
              .select("id_client", "segment", "ville", "region")

    g = (
        s.join(cu, on="id_client", how="left")
        .groupBy("id_client", "segment", "ville", "region", "annee")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.countDistinct("id_produit").alias("nb_produits_distincts"),
            F.sum("quantite").alias("total_articles"),
            F.round(F.sum("montant"), 2).alias("total_depenses"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.round(F.min("montant"), 2).alias("min_commande"),
            F.round(F.max("montant"), 2).alias("max_commande"),
            F.min("date").alias("premiere_commande"),
            F.max("date").alias("derniere_commande"),
        )
        .withColumn("recence_jours",
            F.datediff(F.current_date(), F.col("derniere_commande")))
        .withColumn("frequence_cat",
            F.when(F.col("nb_commandes") >= 10, "Très fréquent")
             .when(F.col("nb_commandes") >= 5,  "Fréquent")
             .when(F.col("nb_commandes") >= 2,  "Occasionnel")
             .otherwise("Unique"))
    )

    g.writeTo("lakehouse.gold.gold_customer_basket") \
     .partitionedBy("annee") \
     .createOrReplace()
    log.info(f"  ✓ gold_customer_basket ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 6 — Taux de retour par produit
# ─────────────────────────────────────────────────────────────
def gold_return_rate(spark):
    log.info("Gold → gold_return_rate")

    s  = spark.table("lakehouse.silver.silver_sales")
    r  = spark.table("lakehouse.silver.silver_returns")
    pr = spark.table("lakehouse.silver.silver_products") \
              .select("id_produit", "nom_produit", "categorie", "marque")

    ventes = (
        s.groupBy("id_produit", "annee", "mois")
         .agg(F.sum("quantite").alias("qte_vendue"),
              F.count("id_vente").alias("nb_ventes"))
    )

    retours = (
        r.withColumn("annee", F.year("date_retour"))
         .withColumn("mois",  F.month("date_retour"))
         .groupBy("id_produit", "annee", "mois")
         .agg(F.sum("quantite_retournee").alias("qte_retournee"),
              F.count("id_retour").alias("nb_retours"))
    )

    g = (
        ventes
        .join(retours, on=["id_produit", "annee", "mois"], how="left")
        .join(pr, on="id_produit", how="left")
        .fillna(0, subset=["qte_retournee", "nb_retours"])
        .withColumn("taux_retour_pct",
            F.when(F.col("qte_vendue") > 0,
                F.round(F.col("qte_retournee") / F.col("qte_vendue") * 100, 2)
            ).otherwise(0.0))
        .withColumn("niveau_retour",
            F.when(F.col("taux_retour_pct") > 20, "Critique")
             .when(F.col("taux_retour_pct") > 10, "Élevé")
             .when(F.col("taux_retour_pct") > 5,  "Normal")
             .otherwise("Faible"))
    )

    g.writeTo("lakehouse.gold.gold_return_rate") \
     .partitionedBy("annee", "mois") \
     .createOrReplace()
    log.info(f"  ✓ gold_return_rate ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 7 — Évolution par catégorie
# ─────────────────────────────────────────────────────────────
def gold_sales_by_category(spark):
    log.info("Gold → gold_sales_by_category")

    s  = spark.table("lakehouse.silver.silver_sales")
    pr = spark.table("lakehouse.silver.silver_products") \
              .select("id_produit", "categorie", "marque")

    g = (
        s.join(pr, on="id_produit", how="left")
        .groupBy("categorie", "marque", "annee", "mois", "trimestre")
        .agg(
            F.count("id_vente").alias("nb_commandes"),
            F.sum("quantite").alias("total_quantite"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.countDistinct("id_client").alias("clients_uniques"),
        )
    )

    # Croissance MoM par catégorie
    w = Window.partitionBy("categorie").orderBy("annee", "mois")
    g = g.withColumn("ca_mois_prec", F.lag("chiffre_affaires", 1).over(w))
    g = g.withColumn("croissance_mom_pct",
        F.when((F.col("ca_mois_prec").isNotNull()) & (F.col("ca_mois_prec") != 0),
            F.round((F.col("chiffre_affaires") - F.col("ca_mois_prec"))
                    / F.col("ca_mois_prec") * 100, 2)
        ).otherwise(None))

    g.writeTo("lakehouse.gold.gold_sales_by_category") \
     .partitionedBy("annee") \
     .createOrReplace()
    log.info(f"  ✓ gold_sales_by_category ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# GOLD 8 — Segments clients
# ─────────────────────────────────────────────────────────────
def gold_customer_segments(spark):
    log.info("Gold → gold_customer_segments")

    s  = spark.table("lakehouse.silver.silver_sales")
    cu = spark.table("lakehouse.silver.silver_customers") \
              .select("id_client", "segment")
    ch = spark.table("lakehouse.silver.silver_channels") \
              .select("id_canal", "type")

    g = (
        s.join(cu, on="id_client", how="left")
         .join(ch, on="id_canal", how="left")
        .groupBy("segment", "type", "annee", "mois")
        .agg(
            F.countDistinct("id_client").alias("nb_clients"),
            F.count("id_vente").alias("nb_commandes"),
            F.round(F.sum("montant"), 2).alias("chiffre_affaires"),
            F.round(F.avg("montant"), 2).alias("panier_moyen"),
            F.round(F.sum("montant") /
                    F.countDistinct("id_client"), 2).alias("ca_par_client"),
        )
    )

    g.writeTo("lakehouse.gold.gold_customer_segments") \
     .partitionedBy("annee") \
     .createOrReplace()
    log.info(f"  ✓ gold_customer_segments ({g.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    spark = get_spark_session("Gold_KPIs")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 55)
    log.info("DÉMARRAGE — Construction couche Gold")
    log.info("=" * 55)

    ensure_gold(spark)

    jobs = [
        ("gold_sales_daily",       gold_sales_daily),
        ("gold_sales_by_channel",  gold_sales_by_channel),
        ("gold_sales_by_region",   gold_sales_by_region),
        ("gold_top_products",      gold_top_products),
        ("gold_customer_basket",   gold_customer_basket),
        ("gold_return_rate",       gold_return_rate),
        ("gold_sales_by_category", gold_sales_by_category),
        ("gold_customer_segments", gold_customer_segments),
    ]

    errors = []
    for name, fn in jobs:
        try:
            fn(spark)
        except Exception as e:
            log.error(f"ERREUR {name}: {e}")
            errors.append(name)

    log.info("=" * 55)
    log.info(f"TERMINÉ Gold — {len(jobs)-len(errors)}/{len(jobs)} tables")
    if errors:
        log.error(f"Erreurs : {errors}")

    log.info("Tables Gold :")
    spark.sql("SHOW TABLES IN lakehouse.gold").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
