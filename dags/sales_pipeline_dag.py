from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "project-0a33b36a-f359-40ab-93b"
REGION = "us-central1"
BUCKET = "gs://sales-pipeline-demo-261244601320"
METASTORE_SERVICE = f"projects/{PROJECT_ID}/locations/{REGION}/services/sales-metastore"
BQ_DATASET = "sales_processed"

BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": f"{BUCKET}/scripts/sales_transform.py",
        "jar_file_uris": [
            "gs://spark-lib/iceberg/iceberg-spark-runtime-3.3_2.12-1.3.1.jar"
        ],
    },
    "environment_config": {
        "execution_config": {
            "service_account": f"composer-sa@{PROJECT_ID}.iam.gserviceaccount.com",
        },
        "peripherals_config": {
            "metastore_service": METASTORE_SERVICE,
        },
    },
    "runtime_config": {
        "properties": {
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
            "spark.sql.catalog.spark_catalog.warehouse": f"{BUCKET}/warehouse",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
    },
}

# CREATE OR REPLACE so the task is idempotent on every DAG run
BQ_EXTERNAL_TABLES_SQL = f"""
CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BQ_DATASET}.revenue_by_region`
WITH CONNECTION DEFAULT
OPTIONS (format = 'ICEBERG', uris = ['{BUCKET}/processed/revenue_by_region']);

CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BQ_DATASET}.revenue_by_product`
WITH CONNECTION DEFAULT
OPTIONS (format = 'ICEBERG', uris = ['{BUCKET}/processed/revenue_by_product']);

CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BQ_DATASET}.monthly_trend`
WITH CONNECTION DEFAULT
OPTIONS (format = 'ICEBERG', uris = ['{BUCKET}/processed/monthly_trend']);
"""

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "dataproc", "sparksql"],
) as dag:

    run_spark_batch = DataprocCreateBatchOperator(
        task_id="run_spark_batch",
        project_id=PROJECT_ID,
        region=REGION,
        # Batch ID must be unique per run — use execution date
        batch_id="sales-transform-{{ ds_nodash }}",
        batch=BATCH_CONFIG,
    )

    register_bq_tables = BigQueryInsertJobOperator(
        task_id="register_bq_tables",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": BQ_EXTERNAL_TABLES_SQL,
                "useLegacySql": False,
            }
        },
    )

    run_spark_batch >> register_bq_tables
