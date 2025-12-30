"""Data generation module for resiliency testing simulations."""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json


class ResiliencySimulator:
    """Simulates resiliency test data for applications."""

    def __init__(self, num_apps: int = 5, num_tests_per_day: int = 50):
        """
        Initialize the resiliency simulator.

        Args:
            num_apps: Number of applications to generate test data for
            num_tests_per_day: Number of tests to generate per day
        """
        self.num_apps = num_apps
        self.num_tests_per_day = num_tests_per_day
        self.scenarios = [
            "Database Failover",
            "Network Latency",
            "Service Restart",
            "Memory Pressure",
            "CPU Spike",
            "Dependency Timeout",
        ]

    def generate_tests(self, date: datetime) -> List[Dict[str, Any]]:
        """
        Generate test data for a given date.

        Args:
            date: Date for which to generate test data

        Returns:
            List of test records
        """
        tests = []

        for _ in range(self.num_tests_per_day):
            app_id = random.randint(1, self.num_apps)
            scenario = random.choice(self.scenarios)
            status = random.choices(["passed", "failed"], weights=[0.85, 0.15])[0]

            # Duration varies by status and scenario type
            if status == "passed":
                duration_ms = random.randint(100, 5000)  # Normal execution: 0.1-5 seconds
            else:
                # Realistic failure durations based on scenario type (enterprise MTTR benchmarks)
                if scenario == "Database Failover":
                    # Database failover typically takes 2-10 minutes
                    duration_ms = random.randint(120000, 600000)
                elif scenario == "Service Restart":
                    # Service restart typically takes 1-5 minutes
                    duration_ms = random.randint(60000, 300000)
                elif scenario == "Network Latency":
                    # Network issues can take 10-30 minutes to resolve
                    duration_ms = random.randint(600000, 1800000)
                elif scenario == "Memory Pressure":
                    # Memory issues typically take 5-20 minutes to resolve
                    duration_ms = random.randint(300000, 1200000)
                elif scenario == "CPU Spike":
                    # CPU spikes usually recover in 2-10 minutes
                    duration_ms = random.randint(120000, 600000)
                elif scenario == "Dependency Timeout":
                    # Dependency timeouts: 5-15 minutes
                    duration_ms = random.randint(300000, 900000)
                else:
                    # Default fallback for unknown scenarios
                    duration_ms = random.randint(1000, 30000)

            start_time = date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )
            end_time = start_time + timedelta(milliseconds=duration_ms)

            error_message = None
            metadata = {}

            if status == "failed":
                error_messages = [
                    "Timeout waiting for database connection",
                    "Service unavailable",
                    "Memory limit exceeded",
                    "Network unreachable",
                    "Dependency service failed",
                ]
                error_message = random.choice(error_messages)
                metadata["failure_reason"] = error_message
                metadata["retry_count"] = random.randint(1, 3)

            test_record = {
                "app_id": app_id,
                "scenario_name": scenario,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_ms": duration_ms,
                "status": status,
                "error_message": error_message,
                "metadata_json": json.dumps(metadata) if metadata else None,
            }

            tests.append(test_record)

        return tests

    def generate_test_batch(self, num_days: int = 7) -> List[Dict[str, Any]]:
        """
        Generate test data for multiple days.

        Args:
            num_days: Number of days of test data to generate

        Returns:
            List of test records
        """
        all_tests = []
        for i in range(num_days):
            date = datetime.now() - timedelta(days=i)
            tests = self.generate_tests(date)
            all_tests.extend(tests)

        return all_tests
