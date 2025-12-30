"""Airflow DAG for resiliency testing data pipeline."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import sys
import os
import logging

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

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
    execution_date = context["execution_date"]
    logger.info(f"[EXTRACT] Starting data extraction for execution_date={execution_date.date()}")
    
    config = get_airflow_config()
    simulator = ResiliencySimulator(
        num_apps=config["num_applications"],
        num_tests_per_day=config["num_tests_per_day"],
    )
    logger.info(f"[EXTRACT] Configured simulator: num_apps={config['num_applications']}, tests_per_day={config['num_tests_per_day']}")

    # Generate test data for yesterday
    tests = simulator.generate_tests(execution_date)
    logger.info(f"[EXTRACT] Generated {len(tests)} raw test records")

    # Push to XCom for downstream tasks
    context["task_instance"].xcom_push(key="raw_tests", value=tests)
    logger.info(f"[EXTRACT] Successfully pushed {len(tests)} records to XCom")

    return {"status": "success", "test_count": len(tests)}


def transform_test_data(**context) -> dict:
    """Transform and clean raw test data."""
    logger.info("[TRANSFORM] Starting data transformation and enrichment")
    
    # Pull raw tests from XCom
    ti = context["task_instance"]
    raw_tests = ti.xcom_pull(task_ids="extract_data", key="raw_tests")

    if not raw_tests:
        logger.error("[TRANSFORM] No raw tests found in XCom - upstream extract task may have failed")
        return {"status": "failed", "reason": "No raw tests found"}

    logger.info(f"[TRANSFORM] Retrieved {len(raw_tests)} raw test records from XCom")

    # Clean and enrich data
    df = ResiliencyTransformer.clean_tests(raw_tests)
    logger.info(f"[TRANSFORM] Cleaned data: {len(df)} valid records (removed {len(raw_tests) - len(df)} duplicates/invalid)")
    
    df = ResiliencyTransformer.enrich_test_data(df)
    logger.info(f"[TRANSFORM] Enriched {len(df)} records with derived columns (test_hour, test_date, duration_minutes, is_critical_failure)")

    # Convert timestamp columns to ISO strings for XCom serialization (before to_dict)
    df["start_time"] = df["start_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    df["end_time"] = df["end_time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    logger.info("[TRANSFORM] Converted timestamp columns to ISO string format for XCom serialization")

    # Convert DataFrame to list of dictionaries for storage
    records = df.to_dict("records")
    logger.info(f"[TRANSFORM] Serialized {len(records)} records to list of dictionaries")

    ti.xcom_push(key="cleaned_tests", value=records)
    logger.info(f"[TRANSFORM] Successfully pushed {len(records)} transformed records to XCom")

    return {
        "status": "success",
        "cleaned_records": len(records),
        "duplicates_removed": len(raw_tests) - len(records),
    }


def load_raw_data(**context) -> dict:
    """Load cleaned data into raw staging table."""
    logger.info("[LOAD_RAW] Starting raw data load into raw_resiliency_tests")
    
    ti = context["task_instance"]
    cleaned_tests = ti.xcom_pull(task_ids="transform_data", key="cleaned_tests")

    if not cleaned_tests:
        logger.error("[LOAD_RAW] No cleaned tests found in XCom - transform task may have failed")
        return {"status": "failed", "reason": "No cleaned tests found"}

    logger.info(f"[LOAD_RAW] Retrieved {len(cleaned_tests)} cleaned records from XCom")

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

    logger.info(f"[LOAD_RAW] Prepared {len(records_to_insert)} records for database insertion")

    # Insert into database
    with DatabaseConnection() as db:
        rows_inserted = db.insert_records(
            table="raw_resiliency_tests", records=records_to_insert
        )
    
    logger.info(f"[LOAD_RAW] Successfully inserted {rows_inserted} records into raw_resiliency_tests")
    logger.info(f"[LOAD_RAW] Data loaded with scenario_id=NULL (will be enriched in staging layer)")

    return {"status": "success", "rows_inserted": rows_inserted}

def populate_staging(**context) -> dict:
    """Populate staging tables from raw data."""
    logger.info("[STAGING] Starting staging layer enrichment (scenario_id lookup)")
    
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
        db.execute_query(sql)
        
        # Query count of records actually inserted
        count_query = "SELECT COUNT(*) FROM resiliency.stg_resiliency_tests WHERE DATE(created_at) = CURRENT_DATE"
        count_result = db.fetch_query(count_query)
        rows_affected = count_result[0][0] if count_result else 0
    
    logger.info(f"[STAGING] Successfully enriched {rows_affected} records with scenario_id via LEFT JOIN")
    logger.info("[STAGING] Medallion architecture: Raw (immutable) -> Staging (enriched) complete")

    return {"status": "success", "rows_staged": rows_affected}

def generate_metrics(**context) -> dict:
    """Generate daily aggregated metrics."""
    logger.info("[METRICS] Starting daily metrics aggregation from staging layer")
    
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
        logger.warning("[METRICS] No staged tests found for today - staging layer may be empty")
        return {"status": "failed", "reason": "No staged tests found"}

    logger.info(f"[METRICS] Retrieved {len(rows)} enriched test records from stg_resiliency_tests")

    import pandas as pd

    df = pd.DataFrame(rows, columns=["app_id", "scenario_id", "start_time", "end_time", "duration_ms", "status"])
    # Convert ISO strings back to datetime
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["test_date"] = df["start_time"].dt.date
    
    # Add enriched columns needed for metrics calculation
    df["duration_minutes"] = df["duration_ms"] / 60000
    logger.info("[METRICS] Converted duration_ms to duration_minutes for MTTR calculation")

    # Generate metrics
    metrics_df = ResiliencyTransformer.generate_daily_metrics(df)
    logger.info(f"[METRICS] Generated daily metrics for {len(metrics_df)} application-day combinations")
    
    # Log summary metrics
    total_uptime = metrics_df["uptime_percentage"].sum() / len(metrics_df) if len(metrics_df) > 0 else 0
    rto_compliant_count = (metrics_df["rto_compliant"] == True).sum()
    logger.info(f"[METRICS] Summary: avg_uptime={total_uptime:.2f}%, rto_compliant_apps={rto_compliant_count}/{len(metrics_df)}")

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
            "mttr_minutes": float(row["mttr_minutes"]) if not pd.isna(row["mttr_minutes"]) else None,
            "uptime_percentage": float(row["uptime_percentage"]),
            "rto_compliant": bool(row["rto_compliant"]),
        }
        metrics_records.append(record)

    logger.info(f"[METRICS] Serialized {len(metrics_records)} metric records for database insertion")

    # Insert into database
    with DatabaseConnection() as db:
        rows_inserted = db.insert_records(
            table="fact_resiliency_metrics", records=metrics_records
        )
    
    logger.info(f"[METRICS] Successfully inserted {rows_inserted} metric rows into fact_resiliency_metrics")
    logger.info(f"[METRICS] Gold layer (Mart) complete - data ready for reporting and visualization")

    return {"status": "success", "metrics_inserted": rows_inserted}


def validate_data_quality(**context) -> dict:
    """Perform data quality checks."""
    logger.info("[VALIDATE] Starting data quality assurance checks")
    
    ti = context["task_instance"]
    cleaned_tests = ti.xcom_pull(task_ids="transform_data", key="cleaned_tests")

    if not cleaned_tests:
        logger.error("[VALIDATE] No data to validate - transform task may have failed")
        return {"status": "failed", "reason": "No data to validate"}

    logger.info(f"[VALIDATE] Retrieved {len(cleaned_tests)} records for quality validation")

    import pandas as pd

    df = pd.DataFrame(cleaned_tests)

    # Quality checks
    checks = {
        "null_app_ids": df["app_id"].isna().sum(),
        "null_status": df["status"].isna().sum(),
        "invalid_status_values": (~df["status"].isin(["passed", "failed"])).sum(),
        "invalid_duration": (df["duration_ms"] <= 0).sum(),
    }

    # Log quality check results
    logger.info(f"[VALIDATE] Quality check results:")
    logger.info(f"  - NULL app_ids: {checks['null_app_ids']}")
    logger.info(f"  - NULL status values: {checks['null_status']}")
    logger.info(f"  - Invalid status (not passed/failed): {checks['invalid_status_values']}")
    logger.info(f"  - Invalid duration (<=0ms): {checks['invalid_duration']}")

    # All checks should pass
    if any(v > 0 for v in checks.values()):
        logger.warning(f"[VALIDATE] Data quality issues detected - {sum(checks.values())} anomalies found")
        return {"status": "warning", "quality_checks": checks}

    logger.info("[VALIDATE] All data quality checks PASSED - data meets enterprise standards")
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
    extract >> transform >> [validate, load] >> stage >> metrics
