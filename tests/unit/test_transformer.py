"""Unit tests for data transformer."""

import pytest
from datetime import datetime
from src.data_generation.transformers import ResiliencyTransformer


class TestResiliencyTransformer:
    """Test cases for ResiliencyTransformer."""

    def test_validate_valid_record(self):
        """Test validation of a valid record."""
        record = {
            "app_id": 1,
            "scenario_name": "Database Failover",
            "start_time": "2025-01-01T10:00:00",
            "end_time": "2025-01-01T10:00:05",
            "duration_ms": 5000,
            "status": "passed",
        }

        assert ResiliencyTransformer.validate_test_record(record) is True

    def test_validate_missing_required_field(self):
        """Test validation fails with missing required field."""
        record = {
            "app_id": 1,
            "scenario_name": "Database Failover",
            "start_time": "2025-01-01T10:00:00",
            # Missing end_time
            "duration_ms": 5000,
            "status": "passed",
        }

        assert ResiliencyTransformer.validate_test_record(record) is False

    def test_validate_invalid_status(self):
        """Test validation fails with invalid status."""
        record = {
            "app_id": 1,
            "scenario_name": "Database Failover",
            "start_time": "2025-01-01T10:00:00",
            "end_time": "2025-01-01T10:00:05",
            "duration_ms": 5000,
            "status": "unknown",
        }

        assert ResiliencyTransformer.validate_test_record(record) is False

    def test_validate_negative_duration(self):
        """Test validation fails with negative duration."""
        record = {
            "app_id": 1,
            "scenario_name": "Database Failover",
            "start_time": "2025-01-01T10:00:00",
            "end_time": "2025-01-01T10:00:05",
            "duration_ms": -100,
            "status": "passed",
        }

        assert ResiliencyTransformer.validate_test_record(record) is False

    def test_clean_tests_removes_invalid_records(self):
        """Test that clean_tests removes invalid records."""
        tests = [
            {
                "app_id": 1,
                "scenario_name": "Database Failover",
                "start_time": "2025-01-01T10:00:00",
                "end_time": "2025-01-01T10:00:05",
                "duration_ms": 5000,
                "status": "passed",
            },
            {
                "app_id": 2,
                "scenario_name": "Network Latency",
                "start_time": "2025-01-01T11:00:00",
                "duration_ms": 3000,
                "status": "failed",
                # Missing end_time - should be removed
            },
        ]

        df = ResiliencyTransformer.clean_tests(tests)
        assert len(df) == 1
        assert df.iloc[0]["app_id"] == 1

    def test_enrich_test_data_adds_columns(self):
        """Test that enrich_test_data adds expected columns."""
        tests = [
            {
                "app_id": 1,
                "scenario_name": "Database Failover",
                "start_time": "2025-01-01T10:00:00",
                "end_time": "2025-01-01T10:00:05",
                "duration_ms": 5000,
                "status": "passed",
            },
        ]

        df = ResiliencyTransformer.clean_tests(tests)
        df = ResiliencyTransformer.enrich_test_data(df)

        assert "test_hour" in df.columns
        assert "test_date" in df.columns
        assert "duration_minutes" in df.columns
        assert "is_critical_failure" in df.columns

    def test_generate_daily_metrics(self):
        """Test metrics generation."""
        tests = [
            {
                "app_id": 1,
                "scenario_name": "Database Failover",
                "start_time": "2025-01-01T10:00:00",
                "end_time": "2025-01-01T10:00:05",
                "duration_ms": 5000,
                "status": "passed",
            },
            {
                "app_id": 1,
                "scenario_name": "Network Latency",
                "start_time": "2025-01-01T11:00:00",
                "end_time": "2025-01-01T11:01:00",
                "duration_ms": 60000,
                "status": "failed",
            },
        ]

        df = ResiliencyTransformer.clean_tests(tests)
        df = ResiliencyTransformer.enrich_test_data(df)
        metrics = ResiliencyTransformer.generate_daily_metrics(df)

        assert len(metrics) == 1
        assert metrics.iloc[0]["total_tests"] == 2
        assert metrics.iloc[0]["passed_tests"] == 1
        assert metrics.iloc[0]["failed_tests"] == 1
        assert metrics.iloc[0]["uptime_percentage"] == 50.0
