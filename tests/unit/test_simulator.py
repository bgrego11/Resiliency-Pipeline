"""Unit tests for resiliency simulator."""

import pytest
from datetime import datetime
from src.data_generation.resiliency_simulator import ResiliencySimulator


class TestResiliencySimulator:
    """Test cases for ResiliencySimulator."""

    def test_initialization(self):
        """Test simulator initialization."""
        simulator = ResiliencySimulator(num_apps=3, num_tests_per_day=20)
        assert simulator.num_apps == 3
        assert simulator.num_tests_per_day == 20
        assert len(simulator.scenarios) > 0

    def test_generate_tests(self):
        """Test test data generation."""
        simulator = ResiliencySimulator(num_apps=5, num_tests_per_day=10)
        date = datetime(2025, 1, 1)

        tests = simulator.generate_tests(date)

        assert len(tests) == 10
        assert all("app_id" in t for t in tests)
        assert all("status" in t for t in tests)
        assert all(t["status"] in ["passed", "failed"] for t in tests)
        assert all(t["duration_ms"] > 0 for t in tests)

    def test_app_id_range(self):
        """Test that generated app_ids are within expected range."""
        simulator = ResiliencySimulator(num_apps=5, num_tests_per_day=50)
        date = datetime(2025, 1, 1)

        tests = simulator.generate_tests(date)

        assert all(1 <= t["app_id"] <= 5 for t in tests)

    def test_status_distribution(self):
        """Test that failure rate is approximately 15%."""
        simulator = ResiliencySimulator(num_apps=5, num_tests_per_day=1000)
        date = datetime(2025, 1, 1)

        tests = simulator.generate_tests(date)
        failures = sum(1 for t in tests if t["status"] == "failed")
        failure_rate = failures / len(tests)

        # Allow some variance (10-20%)
        assert 0.10 <= failure_rate <= 0.20

    def test_duration_for_failures_longer(self):
        """Test that failed tests have longer durations on average."""
        simulator = ResiliencySimulator(num_apps=5, num_tests_per_day=500)
        date = datetime(2025, 1, 1)

        tests = simulator.generate_tests(date)

        passed_durations = [t["duration_ms"] for t in tests if t["status"] == "passed"]
        failed_durations = [t["duration_ms"] for t in tests if t["status"] == "failed"]

        avg_passed = sum(passed_durations) / len(passed_durations)
        avg_failed = sum(failed_durations) / len(failed_durations)

        assert avg_failed > avg_passed
