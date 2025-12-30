"""Airflow DAG for resiliency testing data pipeline."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.data_generation.resiliency_simulator import ResiliencySimulator
from src.data_generation.transformers import ResiliencyTransformer
from src.database.connection import DatabaseConnection
from src.utils.config import get_airflow_config

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "start_date": datetime(2025, 1, 1),
}

# DAG definition
dag = DAG(
    "resiliency_testing_pipeline",
    default_args=default_args,
    description="Data pipeline for resiliency testing data",
    schedule_interval="@daily",
    catchup=False,
)


def extract_test_data(**context) -> dict:
    """Extract raw test data from simulator."""
    config = get_airflow_config()
    simulator = ResiliencySimulator(
        num_apps=config["num_applications"],
        num_tests_per_day=config["num_tests_per_day"],
    )

    # Generate test data for yesterday
    execution_date = context["execution_date"]
    tests = simulator.generate_tests(execution_date)

    # Push to XCom for downstream tasks
    context["task_instance"].xcom_push(key="raw_tests", value=tests)

    return {"status": "success", "test_count": len(tests)}


def transform_test_data(**context) -> dict:
    """Transform and clean raw test data."""
    # Pull raw tests from XCom
    ti = context["task_instance"]
    raw_tests = ti.xcom_pull(task_ids="extract_data", key="raw_tests")

    if not raw_tests:
        return {"status": "failed", "reason": "No raw tests found"}

    # Clean and enrich data
    df = ResiliencyTransformer.clean_tests(raw_tests)
    df = ResiliencyTransformer.enrich_test_data(df)

    # Convert DataFrame to list of dictionaries for storage
    records = df.to_dict("records")
    
    # Convert pandas Timestamps to ISO strings for XCom serialization
    serializable_records = []
    for record in records:
        serialized_record = {}
        for key, value in record.items():
            if hasattr(value, 'isoformat'):  # datetime-like objects
                serialized_record[key] = value.isoformat()
            elif value is None or isinstance(value, (str, int, float, bool)):
                serialized_record[key] = value
            else:
                serialized_record[key] = str(value)
        serializable_records.append(serialized_record)
    
    ti.xcom_push(key="cleaned_tests", value=serializable_records)

    return {
        "status": "success",
        "cleaned_records": len(serializable_records),
        "duplicates_removed": len(raw_tests) - len(serializable_records),
    }


def load_raw_data(**context) -> dict:
    """Load cleaned data into raw staging table."""
    ti = context["task_instance"]
    cleaned_tests = ti.xcom_pull(task_ids="transform_data", key="cleaned_tests")

    if not cleaned_tests:
        return {"status": "failed", "reason": "No cleaned tests found"}

    # Prepare records for insertion (match table schema)
    records_to_insert = []
    for test in cleaned_tests:
        # Convert ISO strings back to datetime for database
        record = {
            "app_id": test["app_id"],
            "scenario_name": test.get("scenario_name"),  # For JOIN in staging
            "scenario_id": None,  # Will be set via JOIN in staging
            "start_time": test["start_time"],  # Already ISO string, DB will parse
            "end_time": test["end_time"],      # Already ISO string, DB will parse
            "duration_ms": int(test["duration_ms"]),
            "status": test["status"],
            "error_message": test.get("error_message"),
            "metadata_json": test.get("metadata_json"),
        }
        records_to_insert.append(record)

    # Insert into database
    with DatabaseConnection() as db:
        rows_inserted = db.insert_records(
            table="raw_resiliency_tests", records=records_to_insert
        )

    return {"status": "success", "rows_inserted": rows_inserted}

def populate_staging(**context) -> dict:
    """Populate staging tables from raw data."""
    with DatabaseConnection() as db:
        # SQL to join raw with dim_test_scenarios and insert into staging
        sql = """
        INSERT INTO resiliency.stg_resiliency_tests (app_id, scenario_name, scenario_id, start_time, end_time, duration_ms, status, error_message, metadata_json)
        SELECT 
            raw.app_id,
            raw.scenario_name,
            ds.scenario_id,
            raw.start_time,
            raw.end_time,
            raw.duration_ms,
            raw.status,
            raw.error_message,
            raw.metadata_json
        FROM resiliency.raw_resiliency_tests raw
        LEFT JOIN resiliency.dim_test_scenarios ds 
            ON raw.scenario_name = ds.scenario_name
        WHERE raw.created_at::date = CURRENT_DATE
        """
        rows_affected = db.execute_query(sql)
        return {"status": "success", "rows_staged": rows_affected}

def generate_metrics(**context) -> dict:
    """Generate daily aggregated metrics."""
    ti = context["task_instance"]
    # Read enriched data from staging table instead of XCom
    with DatabaseConnection() as db:
        query = """
        SELECT app_id, scenario_id, start_time, end_time, duration_ms, status
        FROM resiliency.stg_resiliency_tests
        WHERE DATE(start_time) = CURRENT_DATE
        ORDER BY start_time
        """
        rows = db.fetch_query(query)
    
    if not rows:
        return {"status": "failed", "reason": "No staged tests found"}



    import pandas as pd

    df = pd.DataFrame(rows, columns=["app_id", "scenario_id", "start_time", "end_time", "duration_ms", "status"])
    # Convert ISO strings back to datetime
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["test_date"] = df["start_time"].dt.date
    
    # Add enriched columns needed for metrics calculation
    df["duration_minutes"] = df["duration_ms"] / 60000

    # Generate metrics
    metrics_df = ResiliencyTransformer.generate_daily_metrics(df)

    # Convert DataFrame to records and serialize datetimes as strings
    metrics_records = []
    for _, row in metrics_df.iterrows():
        record = {
            "app_id": int(row["app_id"]),
            "metric_date": row["test_date"].isoformat() if hasattr(row["test_date"], 'isoformat') else str(row["test_date"]),
            "total_tests": int(row["total_tests"]),
            "passed_tests": int(row["passed_tests"]),
            "failed_tests": int(row["failed_tests"]),
            "avg_duration_ms": int(row["avg_duration_ms"]) if not pd.isna(row["avg_duration_ms"]) else None,
            "uptime_percentage": float(row["uptime_percentage"]),
            "rto_compliant": bool(row["rto_compliant"]),
        }
        metrics_records.append(record)

    # Insert into database
    with DatabaseConnection() as db:
        rows_inserted = db.insert_records(
            table="fact_resiliency_metrics", records=metrics_records
        )

    return {"status": "success", "metrics_inserted": rows_inserted}


def validate_data_quality(**context) -> dict:
    """Perform data quality checks."""
    ti = context["task_instance"]
    cleaned_tests = ti.xcom_pull(task_ids="transform_data", key="cleaned_tests")

    if not cleaned_tests:
        return {"status": "failed", "reason": "No data to validate"}

    import pandas as pd

    df = pd.DataFrame(cleaned_tests)

    # Quality checks
    checks = {
        "null_app_ids": df["app_id"].isna().sum(),
        "null_status": df["status"].isna().sum(),
        "invalid_status_values": (~df["status"].isin(["passed", "failed"])).sum(),
        "invalid_duration": (df["duration_ms"] <= 0).sum(),
    }

    # All checks should pass
    if any(v > 0 for v in checks.values()):
        return {"status": "warning", "quality_checks": checks}

    return {"status": "passed", "quality_checks": checks}


# DAG Tasks

with dag:
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_test_data,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_test_data,
        provide_context=True,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data_quality,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
        provide_context=True,
    )

    stage = PythonOperator(
        task_id="populate_staging",
        python_callable=populate_staging,
        provide_context=True,
    )

    metrics = PythonOperator(
        task_id="generate_metrics",
        python_callable=generate_metrics,
        provide_context=True,
    )

    # Define task dependencies
    extract >> transform >> [validate, load] >> metrics
