"""
=============================================================
SILVER — clean_data.py
=============================================================
Lit les tables Bronze et produit les tables Silver nettoyées :
  - Correction encodage (latin-1 → UTF-8)
  - Suppression doublons
  - Standardisation des types
  - Traitement des nulls
  - Normalisation des colonnes texte

Usage :
  spark-submit /opt/spark-jobs/silver/clean_data.py
=============================================================
"""
import sys
sys.path.insert(0, '/opt/spark-jobs')

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from utils.spark_session import get_spark_session
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("silver_clean")


def fix_encoding(col_name):
    """Corrige les caractères mal encodés fréquents (latin-1 lu comme UTF-8)."""
    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.col(col_name),
                        'FÃ"s', 'Fès'
                    ),
                    'MarrakÃ©ch', 'Marrakech'
                ),
                'Ã©', 'é'
            ),
            'Ã ', 'à'
        )
    )

# ─────────────────────────────────────────────────────────────
# Silver — Customers
# ─────────────────────────────────────────────────────────────
def clean_customers(spark):
    log.info("Silver → silver_customers")
    df = spark.table("lakehouse.bronze.bronze_customers")

    df_clean = (
        df
        .dropDuplicates(["id_client"])
        .filter(F.col("id_client").isNotNull())
        .filter(F.col("email").isNotNull())
        # Correction encodage sur les colonnes texte
        .withColumn("ville",   fix_encoding("ville"))
        .withColumn("region",  fix_encoding("region"))
        .withColumn("nom",     F.initcap(F.trim(F.col("nom"))))
        .withColumn("prenom",  F.initcap(F.trim(F.col("prenom"))))
        .withColumn("email",   F.lower(F.trim(F.col("email"))))
        .withColumn("segment", F.trim(F.col("segment")))
        .withColumn("genre",   F.upper(F.trim(F.col("genre"))))
        # Types
        .withColumn("date_inscription",
                    F.to_date(F.col("date_inscription"), "yyyy-MM-dd"))
        .withColumn("age", F.col("age").cast(IntegerType()))
        # Filtre âges aberrants
        .filter((F.col("age") >= 16) | F.col("age").isNull())
        # Suppression colonnes Bronze techniques
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    df_clean.writeTo("lakehouse.silver.silver_customers").createOrReplace()
    log.info(f"  ✓ silver_customers ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — Products
# ─────────────────────────────────────────────────────────────
def clean_products(spark):
    log.info("Silver → silver_products")
    df = spark.table("lakehouse.bronze.bronze_products")

    df_clean = (
        df
        .dropDuplicates(["id_produit"])
        .filter(F.col("id_produit").isNotNull())
        # Correction encodage catégorie
        .withColumn("categorie",
            F.regexp_replace(
                F.regexp_replace(F.col("categorie"), "A%olectronique", "Électronique"),
                "Ã©lectronique", "Électronique")
        )
        .withColumn("nom_produit", F.trim(F.col("nom_produit")))
        .withColumn("marque",      F.trim(F.col("marque")))
        # Types numériques
        .withColumn("prix",        F.col("prix").cast(DoubleType()))
        .withColumn("prix_achat",  F.col("prix_achat").cast(DoubleType()))
        .withColumn("poids_kg",    F.col("poids_kg").cast(DoubleType()))
        .withColumn("stock_initial", F.col("stock_initial").cast(IntegerType()))
        # Filtre cohérence prix
        .filter(F.col("prix") > 0)
        # Calcul marge
        .withColumn("marge_pct",
            F.when(F.col("prix") > 0,
                F.round((F.col("prix") - F.col("prix_achat")) / F.col("prix") * 100, 2)
            ).otherwise(None)
        )
        # Tranche de prix
        .withColumn("tranche_prix",
            F.when(F.col("prix") < 100,  "Bas")
             .when(F.col("prix") < 500,  "Moyen")
             .when(F.col("prix") < 2000, "Haut")
             .otherwise("Premium")
        )
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo("lakehouse.silver.silver_products") \
        .partitionedBy("categorie") \
        .createOrReplace()
    log.info(f"  ✓ silver_products ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — Sales
# ─────────────────────────────────────────────────────────────
def clean_sales(spark):
    log.info("Silver → silver_sales")
    df = spark.table("lakehouse.bronze.bronze_sales")

    df_clean = (
        df
        .dropDuplicates(["id_vente"])
        .filter(F.col("id_vente").isNotNull())
        .filter(F.col("id_client").isNotNull())
        .filter(F.col("id_produit").isNotNull())
        # Types
        .withColumn("date",         F.to_date(F.col("date"), "yyyy-MM-dd"))
        .withColumn("quantite",     F.col("quantite").cast(IntegerType()))
        .withColumn("prix_unitaire",F.col("prix_unitaire").cast(DoubleType()))
        .withColumn("remise_pct",   F.col("remise_pct").cast(DoubleType()))
        .withColumn("montant",      F.col("montant").cast(DoubleType()))
        # Recalcul sécurisé du montant
        .withColumn("montant_calc",
            F.round(F.col("quantite") * F.col("prix_unitaire")
                    * (1 - F.col("remise_pct") / 100), 2)
        )
        # Normalisation statut
        .withColumn("statut",
            F.regexp_replace(
                F.lower(F.trim(F.col("statut"))),
                "livrã©|livrã@|livrÃ©", "livré")
        )
        # Colonnes temporelles
        .withColumn("annee",     F.year("date"))
        .withColumn("mois",      F.month("date"))
        .withColumn("semaine",   F.weekofyear("date"))
        .withColumn("trimestre", F.quarter("date"))
        # Filtre données aberrantes
        .filter(F.col("quantite") > 0)
        .filter(F.col("montant") > 0)
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo("lakehouse.silver.silver_sales") \
        .partitionedBy("annee", "mois") \
        .createOrReplace()
    log.info(f"  ✓ silver_sales ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — Returns
# ─────────────────────────────────────────────────────────────
def clean_returns(spark):
    log.info("Silver → silver_returns")
    df = spark.table("lakehouse.bronze.bronze_returns")

    df_clean = (
        df
        .dropDuplicates(["id_retour"])
        .filter(F.col("id_retour").isNotNull())
        # Correction encodage motif et statut
        .withColumn("motif",
            F.regexp_replace(
                F.regexp_replace(F.col("motif"),
                    "ArrivÃ©|ArrivÃ@", "Arrivé"),
                "endommagÃ©|endommagÃ@", "endommagé")
        )
        .withColumn("statut_retour",
            F.regexp_replace(
                F.regexp_replace(F.col("statut_retour"),
                    "remboursÃ©|remboursÃ@", "remboursé"),
                "Ã", "")
        )
        # Types
        .withColumn("date_retour",
                    F.to_date(F.col("date_retour"), "yyyy-MM-dd"))
        .withColumn("quantite_retournee",
                    F.col("quantite_retournee").cast(IntegerType()))
        .withColumn("montant_rembourse",
                    F.col("montant_rembourse").cast(DoubleType()))
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo("lakehouse.silver.silver_returns").createOrReplace()
    log.info(f"  ✓ silver_returns ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — Stocks
# ─────────────────────────────────────────────────────────────
def clean_stocks(spark):
    log.info("Silver → silver_stocks")
    df = spark.table("lakehouse.bronze.bronze_stocks")

    df_clean = (
        df
        .dropDuplicates(["id_stock"])
        .filter(F.col("id_produit").isNotNull())
        .withColumn("depot", fix_encoding("depot"))
        .withColumn("quantite_disponible",
                    F.col("quantite_disponible").cast(IntegerType()))
        .withColumn("quantite_reservee",
                    F.col("quantite_reservee").cast(IntegerType()))
        .withColumn("seuil_reappro",
                    F.col("seuil_reappro").cast(IntegerType()))
        .withColumn("date_maj",
                    F.to_date(F.col("date_maj"), "yyyy-MM-dd"))
        # Calcul stock net
        .withColumn("stock_net",
            F.col("quantite_disponible") - F.col("quantite_reservee"))
        .withColumn("en_rupture",
            F.col("stock_net") <= 0)
        .withColumn("sous_seuil",
            F.col("stock_net") <= F.col("seuil_reappro"))
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo("lakehouse.silver.silver_stocks").createOrReplace()
    log.info(f"  ✓ silver_stocks ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — Channels
# ─────────────────────────────────────────────────────────────
def clean_channels(spark):
    log.info("Silver → silver_channels")
    df = spark.table("lakehouse.bronze.bronze_channels")

    df_clean = (
        df
        .dropDuplicates(["id_canal"])
        .withColumn("nom",    fix_encoding("nom"))
        .withColumn("ville",  fix_encoding("ville"))
        .withColumn("region", fix_encoding("region"))
        .withColumn("type",   F.lower(F.trim(F.col("type"))))
        .drop("_ingested_at", "_source_file", "_layer")
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo("lakehouse.silver.silver_channels").createOrReplace()
    log.info(f"  ✓ silver_channels ({df_clean.count()} lignes)")

# ─────────────────────────────────────────────────────────────
# Silver — FakeStore
# ─────────────────────────────────────────────────────────────
def clean_fakestore(spark):
    log.info("Silver → silver_fakestore")

    df = spark.table("lakehouse.bronze.bronze_fakestore")

    df_clean = (
        df
        .dropDuplicates(["id"])
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("rating_rate",
                    F.col("rating.rate").cast(DoubleType()))
        .withColumn("rating_count",
                    F.col("rating.count").cast(IntegerType()))
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo(
        "lakehouse.silver.silver_fakestore"
    ).createOrReplace()

    log.info(f"✓ silver_fakestore ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# Silver — DummyJSON
# ─────────────────────────────────────────────────────────────
def clean_dummyjson(spark):
    log.info("Silver → silver_dummyjson")

    df = spark.table("lakehouse.bronze.bronze_dummyjson")

    df_clean = (
        df
        .dropDuplicates(["id"])
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("rating", F.col("rating").cast(DoubleType()))
        .withColumn("stock", F.col("stock").cast(IntegerType()))
        .withColumn("_silver_at", F.current_timestamp())
    )

    df_clean.writeTo(
        "lakehouse.silver.silver_dummyjson"
    ).createOrReplace()

    log.info(f"✓ silver_dummyjson ({df_clean.count()} lignes)")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    spark = get_spark_session("Silver_Clean_Data")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 55)
    log.info("DÉMARRAGE — Nettoyage Silver")
    log.info("=" * 55)

    jobs = [
        ("customers", clean_customers),
        ("products",  clean_products),
        ("sales",     clean_sales),
        ("returns",   clean_returns),
        ("stocks",    clean_stocks),
        ("channels",  clean_channels),
        ("fakestore", clean_fakestore),
        ("dummyjson", clean_dummyjson),      
    ]

    errors = []
    for name, fn in jobs:
        try:
            fn(spark)
        except Exception as e:
            log.error(f"ERREUR {name}: {e}")
            errors.append(name)

    log.info("=" * 55)
    log.info(f"TERMINÉ Silver — {len(jobs)-len(errors)}/{len(jobs)} tables")
    if errors:
        log.error(f"Erreurs : {errors}")

    log.info("Tables Silver :")
    spark.sql("SHOW TABLES IN lakehouse.silver").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
