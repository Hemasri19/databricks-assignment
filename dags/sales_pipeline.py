from datetime import datetime, timedelta
import os
import time
from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from scripts.pandas_injestion import run_ingestion
from utils.email_helper import notify_email_onfailure
from utils.logger import get_logger
import base64

logger = get_logger(__name__)

default_args = {
    "owner": "Global Sales Analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

JOB_ID = Variable.get("databricks_job_id")
CATALOG_NAME = Variable.get("catalog_name")
SCHEMA_NAME = Variable.get("schema")


@dag(
    dag_id="sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["production", "pandas", "pyspark"],
    description="End-to-End Global Sales ETL Pipeline",
)
def sales_pipeline():

    @task
    def run_pandas_ingestion():
        logger.info("Starting Pandas ingestion process...")
        try:
            run_ingestion()
            logger.info("Pandas ingestion completed successfully.")
        except Exception as e:
            logger.error(f"Pandas ingestion failed: {e}")
            raise AirflowFailException(str(e))
        finally:
            logger.info("Pandas ingestion process finished.")

    @task
    def upload_to_dbfs():
        local_path = "data/silver/sales_silver.parquet"
        dbfs_path = "/FileStore/silver/sales_silver.parquet"
        chunk_size = 1024 * 1024  # 1MB

        if not os.path.exists(local_path):
            raise AirflowFailException("Silver parquet file not found!")

        hook = DatabricksHook(databricks_conn_id="databricks_id")

        # Create file in DBFS
        response = hook._do_api_call(
            ("POST", "api/2.0/dbfs/create"),
            {"path": dbfs_path, "overwrite": True}
        )
        handle = response["handle"]

        # Upload file in chunks
        with open(local_path, "rb") as f:
            while chunk := f.read(chunk_size):
                encoded_chunk = base64.b64encode(chunk).decode("utf-8")
                hook._do_api_call(
                    ("POST", "api/2.0/dbfs/add-block"),
                    {"handle": handle, "data": encoded_chunk}
                )

        # Close file
        hook._do_api_call(("POST", "api/2.0/dbfs/close"), {"handle": handle})
        logger.info("File uploaded to DBFS successfully (streaming mode).")

    @task
    def monitor_databricks_job(job_id: int, databricks_conn_id: str):
        hook = DatabricksHook(databricks_conn_id=databricks_conn_id)

        # Trigger job
        response = hook._do_api_call(("POST", "api/2.1/jobs/run-now"), {"job_id": job_id})
        run_id = response.get("run_id")
        if not run_id:
            raise AirflowFailException("Failed to start Databricks job; no run_id returned.")

        logger.info(f"Databricks job triggered with run_id: {run_id}")

        # Poll until job TERMINATED
        while True:
            run_status = hook._do_api_call(("GET", "api/2.1/jobs/runs/get"), {"run_id": run_id})
            state = run_status.get("state", {})
            life_cycle_state = state.get("life_cycle_state")
            result_state = state.get("result_state")

            logger.info(f"Run ID {run_id}: Life cycle = {life_cycle_state}, Result = {result_state}")

            if life_cycle_state == "TERMINATED":
                if result_state != "SUCCESS":
                    notify_email_onfailure(
                        subject="Databricks Job Failed",
                        message=f"Run ID: {run_id}\nResult: {result_state}"
                    )
                    raise AirflowFailException(f"Databricks job failed: {result_state}")
                break

            time.sleep(30)  # poll interval

        logger.info(f"Databricks job {run_id} completed successfully.")
        return True

    # DAG task sequence
    ingestion = run_pandas_ingestion()
    upload_task = upload_to_dbfs()
    monitor_task = monitor_databricks_job(JOB_ID, "databricks_id")

    ingestion >> upload_task >> monitor_task


sales_pipeline()