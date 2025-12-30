# Resiliency Testing Data Pipeline

A comprehensive data pipeline project demonstrating end-to-end data engineering capabilities: synthetic data generation, transformation, orchestration, SQL-based storage, and reporting.

## Project Overview

This project simulates resiliency testing data for multiple applications and builds a production-ready pipeline that:
- Generates synthetic test data with realistic failure patterns
- Performs data quality checks and transformations
- Loads data into PostgreSQL with proper schema design
- Orchestrates workflows with Apache Airflow
- Aggregates metrics for reporting and analysis

## Tech Stack

- **Language**: Python 3.10+
- **Orchestration**: Apache Airflow 2.7.3
- **Database**: PostgreSQL 15
- **Containerization**: Docker & Docker Compose
- **Data Processing**: Pandas, NumPy
- **Data Access**: psycopg2, SQLAlchemy

## Architecture

```
resiliency-pipeline/
├── airflow/                    # Airflow DAGs and configuration
│   └── dags/
│       └── resiliency_pipeline.py  # Main orchestration DAG
├── src/                        # Python source code
│   ├── data_generation/        # Data simulator and transformers
│   ├── database/               # Database connection logic
│   └── utils/                  # Configuration and helpers
├── sql/                        # SQL schemas and queries
│   ├── schema/tables.sql       # Database schema definition
│   ├── seeds/test_data.sql     # Initial dimension data
│   └── queries/                # Reporting queries
├── docker/                     # Docker configuration
├── tests/                      # Unit and integration tests
└── docker-compose.yml          # Multi-container orchestration
```

## Data Model

### Dimensions
- **dim_applications** (AIT): Application metadata with RTO requirements and criticality
- **dim_test_scenarios**: Test scenario definitions and component mapping

### Facts
- **raw_resiliency_tests**: Raw fact table capturing all test results (passed/failed)
- **stg_resiliency_tests**: Staging layer for cleaned and validated data
- **fact_resiliency_metrics**: Aggregated daily metrics by application

## Data Pipeline Flow

```
Extract → Transform → Validate → Load → Aggregate Metrics
  ↓          ↓           ↓        ↓          ↓
Generate  Clean &     Quality   Insert    Daily
  Raw      Enrich      Checks    Raw       Metrics
  Tests    Data                  Data
```

### Extract
Generates synthetic resiliency test data for the execution date:
- Application ID, test scenario, start/end times
- Duration, status (passed/failed), error messages
- 85% pass rate, 15% fail rate by default

### Transform
Cleans and enriches raw test data:
- Validates required fields and data types
- Removes duplicates
- Derives columns: test_hour, test_date, duration_minutes
- Identifies critical failures (>5 min duration)

### Validate
Performs data quality checks:
- Null value detection
- Invalid status values
- Negative/zero duration validation
- Returns warnings if issues found

### Load
Inserts cleaned data into raw tables:
- `raw_resiliency_tests` staging table
- Maintains audit trail of all ingestions

### Aggregate
Generates daily metrics:
- Total tests, passed, failed counts
- Uptime percentage
- Average MTTR (Mean Time To Recovery)
- RTO compliance (99% uptime threshold)

## Getting Started

### Prerequisites
- Docker & Docker Compose
- Git

### Setup

1. **Clone and configure**:
```bash
cd /Users/ben/Resiliency
cp .env.example .env
```

2. **Start containers**:
```bash
docker-compose up -d
```

3. **Initialize database**:
```bash
docker-compose exec postgres psql -U airflow -d resiliency_db -f /docker-entrypoint-initdb.d/init.sql
```

4. **Access Airflow**:
- Web UI: http://localhost:8080
- Default credentials: airflow / airflow

### Running the Pipeline

1. **Trigger DAG manually** (one-time):
```bash
docker-compose exec airflow-scheduler airflow dags test resiliency_testing_pipeline 2025-01-01
```

2. **Enable DAG** in Airflow UI to run on schedule (@daily)

3. **Monitor execution** in Airflow UI or check logs:
```bash
docker-compose logs -f airflow-scheduler
```

## Data Wrangling Features

### Data Validation
- Type checking and field requirement validation
- Status enum validation (passed/failed)
- Duration and app_id positive value checks
- Duplicate detection and removal

### Data Enrichment
- Time-based grouping (hour, date)
- Duration normalization (ms to minutes)
- Critical failure classification
- Derived metrics (uptime %, MTTR)

### Quality Metrics
- Pass/fail rate tracking
- RTO compliance monitoring
- Performance analytics (duration, MTTR)

## SQL Features

### Schema Design
- **Normalized dimensions** for applications and scenarios
- **Single fact table** for all test events (both passed and failed)
- **Proper indexing** for query performance
- **Foreign keys** for referential integrity
- **Check constraints** for data validation

### Reporting Queries
- Daily app uptime and RTO compliance
- RTO compliance summary across applications
- Average MTTR by test scenario
- 7-day trend analysis

## Orchestration with Airflow

### DAG Features
- **Task Groups**: Logical grouping of related tasks
- **XCom**: Inter-task data passing (raw → cleaned tests)
- **Error Handling**: Retry logic and failure notifications
- **Scheduling**: Daily runs at configurable intervals
- **Task Dependencies**: Proper sequencing of operations

### Key Tasks
1. **extract_data**: Generate test data
2. **transform_data**: Clean and enrich
3. **validate_data**: Quality checks (parallel)
4. **load_raw_data**: Persist raw data (parallel)
5. **generate_metrics**: Aggregate metrics

## Key Highlights for Portfolio

✅ **Data Engineering**
- Custom data generators with realistic distributions
- Pandas-based transformation pipelines
- Proper data validation and error handling

✅ **Python**
- Object-oriented design (Simulator, Transformer, DatabaseConnection)
- Context managers for resource management
- Type hints and comprehensive documentation
- Modular, testable code structure

✅ **SQL**
- Complex schema with relationships
- Window functions and CTEs for aggregation
- Performance indexing strategies
- Data integrity constraints

✅ **Orchestration**
- DAG design with task dependencies
- XCom for inter-task communication
- Error handling and retry logic
- Monitoring and logging

✅ **DevOps/Docker**
- Multi-container setup (Airflow, Postgres, Scheduler)
- Volume management for persistence
- Environment configuration
- Health checks and service dependencies

## Future Enhancements

### Short Term
- Add more test scenarios and applications
- Implement Metabase dashboard for reporting
- Add unit and integration tests
- CI/CD pipeline setup

### Medium Term
- Real data integration (connect to actual test systems)
- Advanced metrics (SLA tracking, anomaly detection)
- Data archival strategy for historical data
- Alert system for RTO violations

### Long Term
- Stream processing with Kafka for real-time data
- Machine learning for failure prediction
- Multi-region deployment
- Advanced BI with Looker or Tableau

## Testing

### Run Unit Tests
```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/unit -v
```

### Run Integration Tests
```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/integration -v
```

## Troubleshooting

### Database Connection Issues
```bash
# Check Postgres is running
docker-compose logs postgres

# Test connection manually
docker-compose exec postgres psql -U airflow -d resiliency_db -c "SELECT 1"
```

### Airflow DAG Not Appearing
```bash
# Refresh DAGs
docker-compose exec airflow-webserver airflow dags list

# Check for syntax errors
docker-compose exec airflow-webserver airflow dags test resiliency_testing_pipeline 2025-01-01
```

### View Logs
```bash
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
docker-compose logs postgres
```

## License

MIT License - Feel free to use this as a portfolio project

## Contact

Created by Ben | January 2025
