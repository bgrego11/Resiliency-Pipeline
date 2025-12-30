"""Data transformation module for wrangling raw resiliency test data."""

import pandas as pd
from datetime import datetime
from typing import List, Dict, Any
import json


class ResiliencyTransformer:
    """Handles data wrangling and transformation of resiliency test data."""

    @staticmethod
    def validate_test_record(record: Dict[str, Any]) -> bool:
        """
        Validate a test record for required fields and data types.

        Args:
            record: Test record to validate

        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            "app_id",
            "scenario_name",
            "start_time",
            "end_time",
            "duration_ms",
            "status",
        ]

        # Check all required fields are present
        if not all(field in record for field in required_fields):
            return False

        # Validate status
        if record["status"] not in ["passed", "failed"]:
            return False

        # Validate duration is positive
        if record["duration_ms"] <= 0:
            return False

        # Validate app_id is positive integer
        if not isinstance(record["app_id"], int) or record["app_id"] <= 0:
            return False

        return True

    @staticmethod
    def clean_tests(tests: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Clean and validate test data, returning as DataFrame.

        Args:
            tests: List of raw test records

        Returns:
            Cleaned DataFrame
        """
        # Filter out invalid records
        valid_tests = [t for t in tests if ResiliencyTransformer.validate_test_record(t)]

        df = pd.DataFrame(valid_tests)

        # Convert timestamps to datetime
        df["start_time"] = pd.to_datetime(df["start_time"])
        df["end_time"] = pd.to_datetime(df["end_time"])

        # Remove duplicates based on key columns - presupposed an app cannot conduct duplicate scenario test in 24hrs
        df = df.drop_duplicates(
            subset=["app_id", "scenario_name", "start_time", "status"], keep="first"
        )

        # Sort by start_time
        df = df.sort_values("start_time")

        return df

    @staticmethod
    def enrich_test_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich test data with computed columns.

        Args:
            df: Test DataFrame

        Returns:
            Enriched DataFrame
        """
        df_copy = df.copy()

        # Add test_hour for time-based analysis
        df_copy["test_hour"] = df_copy["start_time"].dt.hour

        # Add test_date for daily aggregation
        df_copy["test_date"] = df_copy["start_time"].dt.date

        # Add duration_minutes
        df_copy["duration_minutes"] = df_copy["duration_ms"] / 60000

        # Add is_critical_failure (failed tests with duration > 5 minutes)
        df_copy["is_critical_failure"] = (df_copy["status"] == "failed") & (
            df_copy["duration_minutes"] > 5
        )

        return df_copy

    @staticmethod
    def generate_daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate daily aggregated metrics by application.

        Args:
            df: Enriched test DataFrame

        Returns:
            Daily metrics DataFrame
        """
        metrics = (
            df.groupby(["test_date", "app_id"])
            .agg(
                total_tests=("status", "count"),
                passed_tests=("status", lambda x: (x == "passed").sum()),
                failed_tests=("status", lambda x: (x == "failed").sum()),
                avg_duration_ms=("duration_ms", "mean"),
                max_duration_ms=("duration_ms", "max"),
                mttr_minutes=("duration_minutes", lambda x: x[df["status"] == "failed"].mean()),
            )
            .reset_index()
        )

        # Calculate uptime percentage
        metrics["uptime_percentage"] = (
            metrics["passed_tests"] / metrics["total_tests"] * 100
        ).round(2)

        # Assume RTO compliant if uptime > 99%
        metrics["rto_compliant"] = metrics["uptime_percentage"] >= 99.0

        # Fill NaN values
        metrics["mttr_minutes"] = metrics["mttr_minutes"].fillna(0).round(2)

        return metrics
