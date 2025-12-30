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

## Data Model (Medallion Architecture)

### Dimensions
- **dim_applications** (AIT): 5 sample applications with RTO requirements (30-480 min), criticality flags, and owner teams
- **dim_test_scenarios**: 6 test scenarios (Database Failover, Network Latency, Service Restart, Memory Pressure, CPU Spike, Dependency Timeout)

### Facts (Medallion Layers)
- **raw_resiliency_tests** (Bronze): Raw fact table with all test results as received, including scenario_name for traceability
- **stg_resiliency_tests** (Silver): Enriched staging layer 
- **fact_resiliency_metrics** (Gold): Daily aggregated metrics by application with uptime %, MTTR, RTO compliance

### Key Design Decisions
- **Separate test_ids**: Raw and staging have independent auto-generated IDs to maintain immutability of raw layer
- **Immutable raw layer**: Audit trail preserved, supports data lineage and re-processing

## Data Pipeline Flow

### End-to-End Architecture
```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────┐     ┌─────────────┐
│   Extract   │────▶│  Transform   │────▶│  Validate    │────▶│   Load   │────▶│  Populate   │────▶┌─────────────┐
│   (Generate)│     │   (Enrich)   │     │  (Quality)   │     │  (Raw)   │     │  (Staging)  │     │  Aggregate  │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────┘     └─────────────┘     │  (Metrics)  │
                                                   │                                     │          └─────────────┘
                                                   └──────────────┬───────────────────────┘
                                                     (Parallel Execution)
```

### Task Breakdown

#### 1. **Extract Data** (extract_data)
Generates synthetic resiliency test data:
- 50 tests per day distributed across 5 applications
- Random scenarios from 6 available options
- 85% pass rate, 15% fail rate distribution
- Varied durations: passed (100-5000ms), failed (1000-30000ms)
- Random error messages and metadata for failed tests

#### 2. **Transform Data** (transform_data)
Cleans and enriches data in-memory (XCom-based):
- Validates required fields and data types
- Removes duplicate test records
- Converts timestamps to ISO strings for XCom serialization
- Derives columns: test_hour, test_date, duration_minutes, is_critical_failure
- Outputs: Cleaned records pushed to XCom for downstream tasks

#### 3. **Validate Data Quality** (validate_data) - *Parallel*
Data quality checks on transformed data:
- Detects null app_ids and status values
- Validates status enum (passed/failed only)
- Checks duration_ms is positive
- Returns: Quality check results or warnings

#### 4. **Load Raw Data** (load_raw_data) - *Parallel*
Persists cleaned data to raw table:
- Inserts into `raw_resiliency_tests` with scenario_id = NULL
- Includes scenario_name for later enrichment
- Maintains immutable audit trail
- Returns: Number of rows inserted

#### 5. **Populate Staging** (populate_staging)
Enriches raw data via SQL transformation:
- SQL LEFT JOIN: raw_resiliency_tests ⟕ dim_test_scenarios on scenario_name
- Inserts enriched records into `stg_resiliency_tests` with populated scenario_id
- Auto-generates new test_ids in staging (separate from raw)
- Returns: Number of rows staged

#### 6. **Generate Metrics** (generate_metrics)
Aggregates daily metrics from staging:
- Reads enriched data from `stg_resiliency_tests`
- Groups by app_id and date
- Calculates: total_tests, passed/failed counts, uptime %, MTTR, RTO compliance
- Inserts into `fact_resiliency_metrics`
- Returns: Number of metric rows created

## Operational Commands

### Daily Monitoring
```bash
# Check if containers are running
docker-compose ps

# View scheduler logs (confirms DAG is executing daily)
docker-compose logs -f airflow-scheduler | tail -50

# View webserver logs (UI activity)
docker-compose logs airflow-webserver | tail -30

# View database logs
docker-compose logs postgres | tail -20
```

### Data Management
```bash
# Clear test data (keep dimension tables for fresh start)
docker-compose exec -T postgres psql -U airflow -d resiliency_db -c "
TRUNCATE TABLE resiliency.fact_resiliency_metrics;
TRUNCATE TABLE resiliency.stg_resiliency_tests;
TRUNCATE TABLE resiliency.raw_resiliency_tests;
"

# Count records across layers
docker-compose exec -T postgres psql -U airflow -d resiliency_db -c "
SELECT 
  'raw_resiliency_tests' as layer, COUNT(*) as record_count FROM resiliency.raw_resiliency_tests
UNION ALL
SELECT 'stg_resiliency_tests', COUNT(*) FROM resiliency.stg_resiliency_tests
UNION ALL
SELECT 'fact_resiliency_metrics', COUNT(*) FROM resiliency.fact_resiliency_metrics
"
```

### Container Management
```bash
# Stop all containers
docker-compose down

# Restart just the scheduler (if DAG was updated)
docker-compose restart airflow-scheduler

# View container resource usage
docker stats
```

### Prerequisites
- Docker & Docker Compose
- macOS (or Linux with similar shell)
- ~2GB disk space for database and Airflow metadata

### Quick Start

**1. Clone and configure**:
```bash
cd /Users/ben/Resiliency
cp .env.example .env
```

**2. Start all services**:
```bash
docker-compose up -d
```

**3. Verify containers are healthy**:
```bash
docker-compose ps
# Should show: postgres (healthy), airflow-webserver (running), airflow-scheduler (running)
```

**4. Access Airflow UI**:
- URL: http://localhost:8080
- Username: `admin`
- Password: `airflow2`

**5. Monitor DAG execution**:
- Click on `resiliency_testing_pipeline` in the DAG list
- View task instances and logs
- DAG runs daily and accumulates data over time

### Database Access

Query the database directly:
```bash
# Access Postgres CLI
docker-compose exec -T postgres psql -U airflow -d resiliency_db

# View raw test data
SELECT COUNT(*) FROM resiliency.raw_resiliency_tests;
SELECT COUNT(*) FROM resiliency.stg_resiliency_tests;

# View aggregated metrics
SELECT * FROM resiliency.fact_resiliency_metrics ORDER BY metric_date DESC LIMIT 10;
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

## Architecture Highlights

### Medallion Data Architecture
This project implements the industry-standard **Medallion Architecture**:

```
Raw (Bronze)  ──────────────→  Staging (Silver)  ──────────────→  Mart (Gold)
═════════════                  ════════════════                    ════════════
Immutable                      Enriched                            Aggregated
Audit Trail                    Cleaned                             Metrics
scenario_id=NULL               scenario_id filled                  Daily KPIs
test_ids: 1,2,3...            test_ids: 1,2,3...                  uptime %
                                                                    MTTR
                               (via SQL LEFT JOIN)                  RTO compliance
```

**Benefits of This Approach:**
- ✅ Raw layer never changes → complete audit trail
- ✅ Staging handles all business logic transformations
- ✅ Separate test_ids maintain layer independence
- ✅ Easy to re-process staging without losing raw data
- ✅ Scales to multiple downstream marts

### Python Code Quality
- **Object-oriented design**: Reusable Simulator, Transformer, DatabaseConnection classes
- **Context managers**: Automatic resource cleanup (database connections)
- **Type hints**: Full type annotations for better IDE support and documentation
- **Error handling**: Validation, logging, and graceful failure modes
- **Modular structure**: Clear separation of concerns (generation, transformation, persistence)

### SQL Best Practices
- **Foreign keys**: Referential integrity between dimensions and facts
- **Check constraints**: Data validation at database level
- **Strategic indexes**: On app_id, start_time, status for query performance
- **Proper normalization**: Separated dimension and fact tables
- **Date-based partitioning**: WHERE DATE(start_time) = CURRENT_DATE for efficiency

## Portfolio Showcase

### Data Engineering Capabilities
✅ **Data Generation** - Synthetic data with realistic distributions (85% pass rate, variable durations)
✅ **Data Validation** - Multi-layer checks (required fields, type validation, range validation)
✅ **Data Transformation** - Column derivation, enrichment, aggregation using pandas
✅ **ETL Pipeline** - Extract → Transform → Load with error handling
✅ **Data Architecture** - Medallion pattern with raw, staging, and mart layers

### Python Skills
✅ **OOP Design** - Simulator, Transformer, DatabaseConnection classes with proper encapsulation
✅ **Type Hints** - Full type annotations across all modules
✅ **Context Managers** - Resource management with `with` statements
✅ **Error Handling** - Validation, logging, graceful degradation
✅ **Pandas Proficiency** - DataFrames, aggregation, data wrangling

### SQL Mastery
✅ **Schema Design** - Normalized dimensions, fact tables, proper keys and constraints
✅ **Complex Queries** - LEFT JOINs, aggregation, GROUP BY operations
✅ **Performance** - Strategic indexing and query optimization
✅ **Data Integrity** - Foreign keys, check constraints, cascading operations
✅ **Best Practices** - Comments, proper naming, logical organization

### Orchestration & DevOps
✅ **Apache Airflow** - DAG design, task dependencies, error handling
✅ **Docker & Compose** - Multi-container orchestration, volume management, health checks
✅ **CI/DevOps** - Environment configuration, secrets management, container networking
✅ **Monitoring** - Logging, health checks, operational commands

## Next Steps: Adding Reporting

The pipeline is data-ready for visualization. Next steps to complete the portfolio:

### Option 1: Metabase (Recommended for Portfolio)
- Lightweight, open-source BI tool
- Docker image available: `docker pull metabase/metabase`
- Pre-built dashboards and SQL-based reporting
- Great for portfolio demonstration

### Option 2: Streamlit (Python-Based)
- Build interactive dashboards with Python
- Perfect complement to data engineering skills
- Deploy easily to cloud

### Option 3: Looker Studio (Free)
- Connect directly to PostgreSQL
- Minimal setup required
- Professional-looking reports

### Recommended Queries for Dashboards
```sql
-- Daily uptime by application
SELECT app_id, metric_date, uptime_percentage FROM resiliency.fact_resiliency_metrics ORDER BY metric_date DESC;

-- RTO compliance status
SELECT app_id, metric_date, rto_compliant FROM resiliency.fact_resiliency_metrics WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days';

-- MTTR trends
SELECT app_id, AVG(mttr_minutes) as avg_mttr FROM resiliency.fact_resiliency_metrics GROUP BY app_id;

-- Failed tests with details
SELECT raw.app_id, raw.scenario_name, raw.start_time, raw.duration_ms, raw.error_message 
FROM resiliency.stg_resiliency_tests raw WHERE raw.status = 'failed' ORDER BY raw.start_time DESC LIMIT 50;
```

## Testing

### Run Unit Tests
```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/unit -v
```

### Run Integration Tests
```bash
docker-compose exec airflow-webserver pytest /opt/airflow/tests/integration -v
```

## Troubleshooting & Common Issues

### DAG Not Executing
```bash
# Check if scheduler is running
docker-compose logs airflow-scheduler | tail -20

# Restart scheduler
docker-compose restart airflow-scheduler

# Check DAG has no syntax errors
docker-compose exec -T airflow-webserver python -m py_compile /opt/airflow/dags/resiliency_pipeline.py
```

### Database Connection Issues
```bash
# Test Postgres connectivity
docker-compose exec -T postgres psql -U airflow -d resiliency_db -c "SELECT 1"

# Check connection pooling
docker-compose exec -T postgres psql -U airflow -d resiliency_db -c "\conninfo"
```

### Staging Table Empty (No Scenario_ids)
- Ensure `populate_staging` task completes successfully
- Verify `dim_test_scenarios` table has data (6 scenarios)
- Check scenario_name values match between raw and dimensions
- Look for LEFT JOIN logging in task logs

### Memory or CPU Issues
```bash
# Monitor container resource usage
docker stats

# Reduce test volume if needed (edit config.yaml)
# Or reduce num_tests_per_day in DAG extract task
```

## License

MIT License - Feel free to use this as a portfolio project

## Contact

Created by Ben | January 2025
