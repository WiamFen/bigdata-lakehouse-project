"""
Airflow DAG — lakehouse_pipeline.py
Orchestrates the full Bronze → Silver → Gold pipeline.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN = "spark_default"  # configured in Airflow connections
SPARK_JARS_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_omnicanal_pipeline",
    default_args=default_args,
    description="Pipeline Bronze → Silver → Gold pour les ventes omnicanales",
    schedule_interval="0 3 * * *",   # every day at 03:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "bigdata", "etl"],
) as dag:

    bronze_ingest = SparkSubmitOperator(
        task_id="bronze_ingest_csv",
        application="/opt/spark-jobs/bronze/ingest_csv.py",
        conn_id=SPARK_CONN,
        packages=SPARK_JARS_PACKAGES,
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )

    silver_clean = SparkSubmitOperator(
        task_id="silver_clean_normalize",
        application="/opt/spark-jobs/silver/silver.py",
        conn_id=SPARK_CONN,
        packages=SPARK_JARS_PACKAGES,
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )

    gold_compute = SparkSubmitOperator(
        task_id="gold_compute_kpis",
        application="/opt/spark-jobs/gold/gold.py",
        conn_id=SPARK_CONN,
        packages=SPARK_JARS_PACKAGES,
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )

    bronze_ingest >> silver_clean >> gold_compute
