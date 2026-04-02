from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

PROJECT_ID = "project-0a33b36a-f359-40ab-93b"
REGION = "us-central1"
CLUSTER_NAME = "sales-pipeline-cluster"
BUCKET = "gs://sales-pipeline-demo-261244601320"
METASTORE_SERVICE = f"projects/{PROJECT_ID}/locations/{REGION}/services/sales-metastore"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "software_config": {"image_version": "2.1-debian11"},
    # Attach Dataproc Metastore so table definitions persist across cluster restarts
    "metastore_config": {"dataproc_metastore_service": METASTORE_SERVICE},
}

SPARKSQL_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_sql_job": {
        "query_file_uri": f"{BUCKET}/scripts/sales_transform.sql",
        # Iceberg runtime jar for Spark 3.3 (bundled with Dataproc 2.1)
        "jar_file_uris": [
            "gs://spark-lib/iceberg/iceberg-spark-runtime-3.3_2.12-1.3.1.jar"
        ],
        "properties": {
            # Register Iceberg as the default Spark catalog
            "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
            "spark.sql.catalog.spark_catalog.type": "hive",
            # GCS warehouse root for Iceberg metadata
            "spark.sql.catalog.spark_catalog.warehouse": f"{BUCKET}/warehouse",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        },
    },
}

BQ_DATASET = "sales_processed"

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

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    run_sparksql = DataprocSubmitJobOperator(
        task_id="run_sparksql",
        job=SPARKSQL_JOB,
        region=REGION,
        project_id=PROJECT_ID,
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

    # TriggerRule.ALL_DONE ensures cluster is deleted even if the job fails
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> run_sparksql >> register_bq_tables >> delete_cluster
