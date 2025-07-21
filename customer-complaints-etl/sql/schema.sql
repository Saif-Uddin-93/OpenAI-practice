-- Customer Complaints ETL Database Schema

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS complaint_resolutions CASCADE;
DROP TABLE IF EXISTS complaints CASCADE;
DROP TABLE IF EXISTS customers CASCADE;
DROP TABLE IF EXISTS complaint_types CASCADE;
DROP TABLE IF EXISTS severity_levels CASCADE;

-- Create lookup tables first
CREATE TABLE severity_levels (
    severity_id SERIAL PRIMARY KEY,
    severity_name VARCHAR(20) NOT NULL UNIQUE,
    severity_level INTEGER NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE complaint_types (
    type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    category VARCHAR(30) NOT NULL,
    description TEXT
);

-- Create customers table
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_uuid VARCHAR(36) NOT NULL UNIQUE,
    age_group VARCHAR(10) NOT NULL,
    region VARCHAR(20) NOT NULL,
    customer_type VARCHAR(20) NOT NULL,
    gender VARCHAR(30) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create main complaints table
CREATE TABLE complaints (
    complaint_id SERIAL PRIMARY KEY,
    complaint_uuid VARCHAR(36) NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    type_id INTEGER NOT NULL REFERENCES complaint_types(type_id),
    severity_id INTEGER NOT NULL REFERENCES severity_levels(severity_id),
    complaint_text TEXT NOT NULL,
    complaint_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create resolutions table
CREATE TABLE complaint_resolutions (
    resolution_id SERIAL PRIMARY KEY,
    complaint_id INTEGER NOT NULL REFERENCES complaints(complaint_id),
    resolution_status VARCHAR(20) NOT NULL DEFAULT 'Open',
    resolution_date TIMESTAMP,
    resolution_notes TEXT,
    resolution_time_hours INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert reference data for severity levels
INSERT INTO severity_levels (severity_name, severity_level, description) VALUES
('Critical', 1, 'Service outage, data breach, safety issues'),
('High', 2, 'Billing errors, major service disruption'),
('Medium', 3, 'Feature requests, minor service issues'),
('Low', 4, 'General inquiries, cosmetic issues');

-- Insert reference data for complaint types
INSERT INTO complaint_types (type_name, category, description) VALUES
('Product Quality', 'Product', 'Issues with product defects or quality'),
('Service Issues', 'Service', 'Problems with customer service experience'),
('Billing Problems', 'Financial', 'Billing errors, payment issues'),
('Delivery Issues', 'Logistics', 'Problems with shipping and delivery'),
('Technical Support', 'Technical', 'Technical assistance and troubleshooting'),
('Account Management', 'Account', 'Account access and management issues'),
('Refund Requests', 'Financial', 'Requests for refunds and returns');

-- Create indexes for better query performance
CREATE INDEX idx_complaints_date ON complaints(complaint_date);
CREATE INDEX idx_complaints_severity ON complaints(severity_id);
CREATE INDEX idx_complaints_type ON complaints(type_id);
CREATE INDEX idx_complaints_customer ON complaints(customer_id);
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_customers_type ON customers(customer_type);
CREATE INDEX idx_resolutions_status ON complaint_resolutions(resolution_status);

-- Create a view for complaint analytics
CREATE VIEW complaint_analytics AS
SELECT 
    c.complaint_id,
    c.complaint_uuid,
    cust.customer_uuid,
    cust.age_group,
    cust.region,
    cust.customer_type,
    cust.gender,
    ct.type_name as complaint_type,
    ct.category as complaint_category,
    sl.severity_name,
    sl.severity_level,
    c.complaint_date,
    cr.resolution_status,
    cr.resolution_date,
    cr.resolution_time_hours,
    CASE 
        WHEN cr.resolution_date IS NOT NULL 
        THEN EXTRACT(EPOCH FROM (cr.resolution_date - c.complaint_date))/3600 
        ELSE NULL 
    END as actual_resolution_hours
FROM complaints c
JOIN customers cust ON c.customer_id = cust.customer_id
JOIN complaint_types ct ON c.type_id = ct.type_id
JOIN severity_levels sl ON c.severity_id = sl.severity_id
LEFT JOIN complaint_resolutions cr ON c.complaint_id = cr.complaint_id;

-- Create a summary view for reporting
CREATE VIEW complaint_summary AS
SELECT 
    DATE_TRUNC('day', complaint_date) as complaint_day,
    region,
    complaint_category,
    severity_name,
    COUNT(*) as complaint_count,
    AVG(resolution_time_hours) as avg_resolution_hours
FROM complaint_analytics
GROUP BY DATE_TRUNC('day', complaint_date), region, complaint_category, severity_name
ORDER BY complaint_day DESC;
