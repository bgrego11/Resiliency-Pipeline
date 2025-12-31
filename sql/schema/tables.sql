-- Create schema
CREATE SCHEMA IF NOT EXISTS resiliency;

-- Dimension: Applications (AIT Table)
CREATE TABLE IF NOT EXISTS resiliency.dim_applications (
    app_id SERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL UNIQUE,
    rto_requirement_minutes INT NOT NULL,
    is_critical BOOLEAN DEFAULT FALSE,
    owner_team VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Test Scenarios
CREATE TABLE IF NOT EXISTS resiliency.dim_test_scenarios (
    scenario_id SERIAL PRIMARY KEY,
    scenario_name VARCHAR(255) NOT NULL UNIQUE,
    scenario_description TEXT,
    component_tested VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw Fact Table: Resiliency Tests
CREATE TABLE IF NOT EXISTS resiliency.raw_resiliency_tests (
    test_id UUID PRIMARY KEY,  -- Unique identifier from source system
    app_id INT NOT NULL,
    scenario_id INT NOT NULL,  -- Source provides normalized ID
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_ms INT NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('passed', 'failed')),
    error_message TEXT,
    metadata_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (app_id) REFERENCES resiliency.dim_applications(app_id),
    FOREIGN KEY (scenario_id) REFERENCES resiliency.dim_test_scenarios(scenario_id),
    UNIQUE (test_id)
);

-- Staging Fact Table: Resiliency Tests (cleaned/validated/enriched)
CREATE TABLE IF NOT EXISTS resiliency.stg_resiliency_tests (
    test_id UUID PRIMARY KEY,  -- Unique identifier from source system
    app_id INT NOT NULL,
    app_name VARCHAR(255),  -- Enriched from dimension for convenience
    scenario_id INT NOT NULL,  -- Direct reference to dimension
    scenario_name VARCHAR(255),  -- Enriched from dimension for convenience
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_ms INT NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('passed', 'failed')),
    error_message TEXT,
    metadata_json JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (app_id) REFERENCES resiliency.dim_applications(app_id),
    FOREIGN KEY (scenario_id) REFERENCES resiliency.dim_test_scenarios(scenario_id),
    UNIQUE (test_id)
);

-- Mart: Resiliency Metrics (aggregated)
CREATE TABLE IF NOT EXISTS resiliency.fact_resiliency_metrics (
    metric_id SERIAL PRIMARY KEY,
    app_id INT NOT NULL,
    metric_date DATE NOT NULL,
    total_tests INT,
    passed_tests INT,
    failed_tests INT,
    uptime_percentage DECIMAL(5,2),
    avg_duration_ms INT,
    mttr_minutes DECIMAL(10,2),
    rto_compliant BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (app_id) REFERENCES resiliency.dim_applications(app_id),
    UNIQUE(app_id, metric_date)
);

-- Create indexes for performance
CREATE INDEX idx_raw_tests_app_id ON resiliency.raw_resiliency_tests(app_id);
CREATE INDEX idx_raw_tests_start_time ON resiliency.raw_resiliency_tests(start_time);
CREATE INDEX idx_raw_tests_status ON resiliency.raw_resiliency_tests(status);
CREATE INDEX idx_stg_tests_app_id ON resiliency.stg_resiliency_tests(app_id);
CREATE INDEX idx_metrics_app_id ON resiliency.fact_resiliency_metrics(app_id);
CREATE INDEX idx_metrics_date ON resiliency.fact_resiliency_metrics(metric_date);

-- Comments for documentation
COMMENT ON TABLE resiliency.dim_applications IS 'Application Information Table (AIT) - Contains metadata about applications under resiliency testing';
COMMENT ON TABLE resiliency.raw_resiliency_tests IS 'Raw fact table containing all test results (passed and failed)';
COMMENT ON TABLE resiliency.stg_resiliency_tests IS 'Staging area for cleaned and validated test data';
COMMENT ON TABLE resiliency.fact_resiliency_metrics IS 'Aggregated metrics by application and date for reporting';
