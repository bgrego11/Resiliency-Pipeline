"""Utility functions and configuration."""

import os
from dotenv import load_dotenv

load_dotenv()


def get_database_config() -> dict:
    """Get database configuration from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5432)),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database": os.getenv("POSTGRES_DB", "resiliency_db"),
    }


def get_airflow_config() -> dict:
    """Get Airflow configuration from environment variables."""
    return {
        "dags_folder": os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags"),
        "data_refresh_interval": os.getenv("DATA_REFRESH_INTERVAL", "daily"),
        "num_applications": int(os.getenv("NUM_APPLICATIONS", 5)),
        "num_tests_per_day": int(os.getenv("NUM_TESTS_PER_DAY", 50)),
    }
