---
name: creating-pipeline-models
description: |
  Creates SparkSQL transformations and Airflow DAGs following project conventions. Use when:
  (1) Creating a new SparkSQL transform or Iceberg output table
  (2) Creating or modifying an Airflow DAG
  (3) Task mentions "create", "build", "add", "write", "new", or "implement" with pipeline, DAG, transform, or table
  (4) Modifying existing transformation logic, columns, joins, or aggregations
  Discovers project conventions before writing. Runs the pipeline to verify output.
---

# SparkSQL + Airflow Pipeline Development

**Read before you write. Run after you write. Verify your output.**

## Critical Rules

1. **ALWAYS run the Dataproc batch** after creating/modifying transforms — syntax check is NOT enough
2. **ALWAYS verify output** after the run using BigQuery or GCS — don't assume success
3. **If the batch fails 3+ times**, stop and reassess your entire approach
4. **NEVER use** `--batch-id` flag — the correct flag is `--batch`
5. **NEVER reference** Iceberg jar version 3.3 — only 3.5 is available in spark-lib

## Project Constants

```
PROJECT_ID:      project-0a33b36a-f359-40ab-93b
BUCKET:          gs://sales-pipeline-demo-261244601320
REGION:          us-central1
METASTORE:       projects/project-0a33b36a-f359-40ab-93b/locations/us-central1/services/sales-metastore
ICEBERG_JAR:     gs://spark-lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
DATAPROC_VERSION: 2.2
SCHEDULER_SA:    composer-sa@project-0a33b36a-f359-40ab-93b.iam.gserviceaccount.com
```

## Workflow

### 1. Understand the Task Requirements

- What columns are needed? List them explicitly.
- What is the grain of the output table (one row per what)?
- What upstream tables does it depend on?
- What is the schedule and SLA?
- Does this need a new DAG or modify an existing one?

### 2. Discover Project Conventions

```bash
# Read existing transforms and DAGs before writing anything
cat scripts/sales_transform.py
cat dags/sales_pipeline_dag.py
```

Learn: Iceberg table patterns, Spark properties, DAG operator usage, naming conventions.

### 3. Check Upstream Data

```bash
# Preview upstream tables before writing SQL
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  'SELECT * FROM `project-0a33b36a-f359-40ab-93b.<dataset>.<table>` LIMIT 10'
```

Understand the shape, grain, and nullability of source data before writing transforms.

### 4. Write the SparkSQL Transform

Follow these conventions:

```python
# Always use CREATE OR REPLACE TABLE with Iceberg
spark.sql(f"""
    CREATE OR REPLACE TABLE <database>.<table_name>
    USING iceberg
    LOCATION '{PROCESSED}/<table_name>'
    TBLPROPERTIES (
        'write.format.default'            = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    AS
    SELECT ...
    FROM <fully_qualified_source_table>
    ...
""")
```

Rules:
- Always use fully qualified table names: `` `project-0a33b36a-f359-40ab-93b.<dataset>.<table>` ``
- Use `CREATE OR REPLACE TABLE ... USING iceberg` — never `INSERT OVERWRITE DIRECTORY`
- Always set `write.format.default = parquet` and `write.parquet.compression-codec = snappy`
- Handle NULLs explicitly in joins and aggregations
- Upload the updated script to GCS after writing:

```bash
gcloud storage cp scripts/<script>.py gs://sales-pipeline-demo-261244601320/scripts/<script>.py
```

### 5. Write the Airflow DAG (if needed)

Follow the existing DAG structure:

```python
# Standard imports
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule

# Always set retries and retry_delay
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Use ds_nodash for unique batch IDs per run
batch_id="<pipeline-name>-{{ ds_nodash }}"

# Always use TriggerRule.ALL_DONE on cleanup tasks
cleanup_task = SomeOperator(
    ...
    trigger_rule=TriggerRule.ALL_DONE,
)
```

### 6. Validate Syntax

```bash
# Check Python syntax on the transform
python3 -c "import ast; ast.parse(open('scripts/<script>.py').read()); print('OK')"

# Check DAG loads without errors
python3 dags/<dag_name>.py
```

### 7. Run the Pipeline — MANDATORY

**This step is REQUIRED. Do NOT skip it.**

```bash
gcloud dataproc batches submit pyspark \
  gs://sales-pipeline-demo-261244601320/scripts/<script>.py \
  --project=project-0a33b36a-f359-40ab-93b \
  --region=us-central1 \
  --batch=<pipeline-name>-$(date +%s) \
  --jars=gs://spark-lib/iceberg/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
  --properties=spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog,spark.sql.catalog.spark_catalog.type=hive,spark.sql.catalog.spark_catalog.warehouse=gs://sales-pipeline-demo-261244601320/warehouse,spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --metastore-service=projects/project-0a33b36a-f359-40ab-93b/locations/us-central1/services/sales-metastore \
  --version=2.2
```

If batch fails:
1. Read the error carefully — check logs in `console.cloud.google.com/dataproc/batches`
2. Fix the specific issue
3. Re-upload the script to GCS and run again
4. **If fails 3+ times, step back and reassess the approach**

### 8. Verify Output — CRITICAL

**Batch success does NOT mean correct output.**

```bash
# Check row count
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  'SELECT COUNT(*) FROM `project-0a33b36a-f359-40ab-93b.<dataset>.<table>`'

# Preview data
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  'SELECT * FROM `project-0a33b36a-f359-40ab-93b.<dataset>.<table>` LIMIT 10'

# Verify Iceberg files exist in GCS
gcloud storage ls gs://sales-pipeline-demo-261244601320/processed/<table>/data/
gcloud storage ls gs://sales-pipeline-demo-261244601320/processed/<table>/metadata/
```

Verify:
- Column names match requirements exactly
- Row count is reasonable (not 0, not unexpectedly large)
- No unexpected NULLs in key columns
- Iceberg metadata folder exists (confirms proper Iceberg write)

### 9. Verify Calculations Against Sample Data

**For transforms with calculations, verify correctness manually:**

```bash
# Pick a specific row and verify the calculation
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  'SELECT * FROM `project-0a33b36a-f359-40ab-93b.<dataset>.<table>`
   WHERE <key_column> = "<known_value>"'

# Cross-check against source
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  'SELECT SUM(quantity * unit_price)
   FROM `project-0a33b36a-f359-40ab-93b.sales_raw.orders`
   WHERE <filter>'
```

For example, if computing `total_revenue`:
1. Pick one region from the output
2. Query `sales_raw.orders` directly for that region
3. Manually sum `quantity * unit_price` — does it match?

### 10. Register BigQuery External Table (if new Iceberg table)

After verifying GCS output, register it as a BQ external table:

```bash
# Get the metadata.json path
gcloud storage ls gs://sales-pipeline-demo-261244601320/processed/<table>/metadata/*.json

# Register as BQ external table using the metadata file path (not the folder)
bq --project_id=project-0a33b36a-f359-40ab-93b query --nouse_legacy_sql \
  "CREATE OR REPLACE EXTERNAL TABLE \`project-0a33b36a-f359-40ab-93b.sales_processed.<table>\`
   OPTIONS (format = 'ICEBERG', uris = ['gs://sales-pipeline-demo-261244601320/processed/<table>/metadata/<file>.metadata.json']);"
```

### 11. Commit to GitHub

```bash
git add scripts/<script>.py dags/<dag>.py
git commit -m "Add <description of what was built>"
git push
```

### 12. Re-review Against Requirements

Before declaring done:
- Does the output schema match the data contract?
- Is the grain correct (one row per what)?
- Are all columns named exactly as specified?
- Is the calculation logic correct?
- Is the DAG schedule correct per the SLA?
- Is the BQ external table registered and queryable?

## Anti-Patterns

- Running `dbt compile` equivalent (syntax only) without running the actual batch
- Not verifying output row counts and data after a successful batch
- Using `INSERT OVERWRITE DIRECTORY` instead of `CREATE OR REPLACE TABLE USING iceberg`
- Using `--batch-id` flag (correct flag is `--batch`)
- Referencing Iceberg jar version 3.3 (only 3.5 available in spark-lib)
- Registering BQ external tables with folder path instead of `metadata.json` file path
- Hardcoding project/bucket values instead of using the constants above
- Uploading script to GCS but forgetting to re-run the batch after changes
- Assuming Iceberg table is correct because batch succeeded — always verify data
