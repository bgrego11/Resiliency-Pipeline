-- Seed data for applications (AIT)
INSERT INTO resiliency.dim_applications (app_name, rto_requirement_minutes, is_critical, owner_team, description)
VALUES
    ('Payment Service', 15, true, 'Backend Team', 'Critical payment processing service'),
    ('User Authentication', 10, true, 'Security Team', 'Identity and access management system'),
    ('Order Management', 30, true, 'Commerce Team', 'Order processing and tracking system'),
    ('Notification Service', 60, false, 'Platform Team', 'Email and SMS notification service'),
    ('Analytics Engine', 120, false, 'Data Team', 'Real-time analytics and reporting system')
ON CONFLICT (app_name) DO NOTHING;

-- Seed data for test scenarios
INSERT INTO resiliency.dim_test_scenarios (scenario_name, scenario_description, component_tested)
VALUES
    ('Database Failover', 'Test failover to replica database', 'Database'),
    ('Network Latency', 'Simulate high network latency', 'Network'),
    ('Service Restart', 'Graceful service restart', 'Service'),
    ('Memory Pressure', 'Test under memory constraints', 'Memory'),
    ('CPU Spike', 'Test under high CPU load', 'CPU'),
    ('Dependency Timeout', 'Test with external service timeout', 'Dependencies')
ON CONFLICT (scenario_name) DO NOTHING;
