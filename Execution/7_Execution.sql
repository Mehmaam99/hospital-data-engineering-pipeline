/*
7. Execution Instructions
7.1 Initial Setup and Data Load
7.2 Data Quality and Monitoring
*/

-- 7.1 Initial Setup and Data Load

-- Run initial full load
EXEC etl.sp_run_full_etl_pipeline;

-- 7.2 Data Quality and Monitoring

-- Check ETL load status
SELECT * FROM etl.v_load_status;

-- Review data quality audit
SELECT * FROM etl.v_data_quality_summary;

-- View business analytics
SELECT * FROM analytics.v_occupancy_trends ORDER BY full_date DESC;
SELECT * FROM analytics.v_department_revenue ORDER BY total_revenue DESC;
SELECT TOP 10 * FROM analytics.v_top_procedures ORDER BY total_revenue DESC;