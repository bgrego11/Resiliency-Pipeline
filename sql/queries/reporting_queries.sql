-- Reporting queries

-- Query 1: Daily app uptime percentage
SELECT
    app_id,
    app_name,
    metric_date,
    total_tests,
    passed_tests,
    failed_tests,
    uptime_percentage,
    rto_compliant
FROM resiliency.fact_resiliency_metrics frm
LEFT JOIN resiliency.dim_applications da ON frm.app_id = da.app_id
ORDER BY metric_date DESC, app_id;

-- Query 2: RTO compliance summary
SELECT
    app_name,
    rto_requirement_minutes,
    is_critical,
    SUM(total_tests) as total_tests_all_time,
    COUNT(CASE WHEN rto_compliant = true THEN 1 END) as compliant_days,
    COUNT(CASE WHEN rto_compliant = false THEN 1 END) as non_compliant_days,
    ROUND(100.0 * COUNT(CASE WHEN rto_compliant = true THEN 1 END) / COUNT(*), 2) as compliance_percentage
FROM resiliency.fact_resiliency_metrics frm
LEFT JOIN resiliency.dim_applications da ON frm.app_id = da.app_id
GROUP BY app_name, rto_requirement_minutes, is_critical
ORDER BY compliance_percentage DESC;

-- Query 3: Average MTTR by scenario
SELECT
    ds.scenario_name,
    ds.component_tested,
    COUNT(rrt.test_id) as test_count,
    ROUND(AVG(rrt.duration_ms)::numeric, 2) as avg_duration_ms,
    ROUND(AVG(rrt.duration_ms) / 60000.0, 2) as avg_duration_minutes,
    SUM(CASE WHEN rrt.status = 'passed' THEN 1 ELSE 0 END) as passed_count,
    SUM(CASE WHEN rrt.status = 'failed' THEN 1 ELSE 0 END) as failed_count,
    ROUND(100.0 * SUM(CASE WHEN rrt.status = 'passed' THEN 1 ELSE 0 END) / COUNT(*)::numeric, 2) as success_rate
FROM resiliency.raw_resiliency_tests rrt
LEFT JOIN resiliency.dim_test_scenarios ds ON rrt.scenario_id = ds.scenario_id
GROUP BY ds.scenario_name, ds.component_tested
ORDER BY test_count DESC;

-- Query 4: Last 7 days trend
SELECT
    metric_date,
    SUM(total_tests) as total_tests_per_day,
    SUM(passed_tests) as total_passed_per_day,
    SUM(failed_tests) as total_failed_per_day,
    ROUND(100.0 * SUM(passed_tests) / SUM(total_tests)::numeric, 2) as overall_uptime_percentage
FROM resiliency.fact_resiliency_metrics
WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY metric_date
ORDER BY metric_date DESC;
