/*
6. Monitoring and Reporting Queries
6.1 Data Quality Monitoring
6.2 Business Intelligence Queries

*/

-- 6.1 Data Quality Monitoring
-- Monitor ETL load status


CREATE VIEW etl.v_load_status AS
SELECT 
    table_name,
    load_status,
    last_successful_load,
    records_loaded,
    DATEDIFF(HOUR, last_successful_load, GETDATE()) as hours_since_last_load,
    CASE 
        WHEN DATEDIFF(HOUR, last_successful_load, GETDATE()) > 24 THEN 'OVERDUE'
        WHEN load_status = 'FAILED' THEN 'FAILED'
        WHEN load_status = 'RUNNING' AND DATEDIFF(MINUTE, updated_date, GETDATE()) > 30 THEN 'STUCK'
        ELSE 'OK'
    END as health_status
FROM etl.load_control;
GO
-- Data quality audit summary
CREATE VIEW etl.v_data_quality_summary AS
SELECT 
    table_name,
    check_name,
    check_result,
    COUNT(*) as check_count,
    MAX(check_date) as last_check_date
FROM etl.data_quality_audit
WHERE check_date >= DATEADD(DAY, -7, GETDATE())
GROUP BY table_name, check_name, check_result;

-- 6.2 Business Intelligence Queries
-- Hospital occupancy trends
GO
CREATE VIEW analytics.v_occupancy_trends AS
SELECT 
    dd.full_date,
    dd.day_name,
    COUNT(CASE WHEN fa.discharge_date_key IS NULL THEN 1 END) as current_occupancy,
    COUNT(fa.admission_key) as total_admissions,
    AVG(CAST(fa.length_of_stay_days AS DECIMAL(8,2))) as avg_length_of_stay
FROM dw.dim_date dd
LEFT JOIN dw.fact_admission fa ON dd.date_key = fa.admission_date_key
WHERE dd.full_date >= DATEADD(DAY, -30, GETDATE())
GROUP BY dd.full_date, dd.day_name, dd.date_key;
GO
-- Department revenue analysis
CREATE VIEW analytics.v_department_revenue AS
SELECT 
    doc.department,
    COUNT(DISTINCT fa.patient_key) as unique_patients,
    COUNT(fa.admission_key) as total_admissions,
    SUM(fa.treatment_cost) as total_revenue,
    AVG(fa.treatment_cost) as avg_revenue_per_admission,
    SUM(fp.cost) as procedure_revenue,
    COUNT(fp.procedure_key) as total_procedures
FROM dw.fact_admission fa
INNER JOIN dw.dim_doctor doc ON fa.doctor_key = doc.doctor_key AND doc.is_current = 1
LEFT JOIN dw.fact_procedure fp ON fa.admission_key = fp.admission_key
INNER JOIN dw.dim_date dd ON fa.admission_date_key = dd.date_key
WHERE dd.full_date >= DATEADD(MONTH, -3, GETDATE())
GROUP BY doc.department;
GO
-- Top procedures by volume and revenue
CREATE VIEW analytics.v_top_procedures AS
SELECT 
    fp.procedure_name,
    fp.procedure_code,
    COUNT(*) as procedure_count,
    SUM(fp.cost) as total_revenue,
    AVG(fp.cost) as avg_cost,
    AVG(fp.duration_minutes) as avg_duration_minutes,
    COUNT(DISTINCT fp.performing_doctor_key) as doctors_performing
FROM dw.fact_procedure fp
INNER JOIN dw.dim_date dd ON fp.procedure_date_key = dd.date_key
WHERE dd.full_date >= DATEADD(MONTH, -6, GETDATE())
GROUP BY fp.procedure_name, fp.procedure_code
-- HAVING COUNT(*) >= 1;
GO

-- Patient demographics analysis
CREATE VIEW analytics.v_patient_demographics AS
SELECT 
    p.gender,
    CASE 
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 18 AND 30 THEN '18-30'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 31 AND 50 THEN '31-50'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 51 AND 70 THEN '51-70'
        ELSE '70+'
    END as age_group,
    p.state,
    p.insurance_provider,
    COUNT(DISTINCT p.patient_key) as patient_count,
    COUNT(fa.admission_key) as total_admissions,
    AVG(fa.treatment_cost) as avg_treatment_cost
FROM dw.dim_patient p
INNER JOIN dw.fact_admission fa ON p.patient_key = fa.patient_key
WHERE p.is_current = 1
GROUP BY p.gender, 
    CASE 
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) < 18 THEN 'Under 18'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 18 AND 30 THEN '18-30'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 31 AND 50 THEN '31-50'
        WHEN DATEDIFF(YEAR, p.date_of_birth, GETDATE()) BETWEEN 51 AND 70 THEN '51-70'
        ELSE '70+'
    END,
    p.state,
    p.insurance_provider;
