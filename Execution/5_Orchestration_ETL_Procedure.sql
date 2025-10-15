/*
5. Master ETL Orchestration
5.1 Main ETL Process

*/

-- 5.1 Main ETL Process

CREATE PROCEDURE etl.sp_run_full_etl_pipeline
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @start_time DATETIME2 = GETDATE();
    DECLARE @step_name VARCHAR(100);
    DECLARE @error_message VARCHAR(1000);
    
    PRINT 'Starting Full ETL Pipeline at: ' + CAST(@start_time AS VARCHAR);
    PRINT '=================================================';
    
    BEGIN TRY
        -- Step 1: Data Quality Checks
        SET @step_name = 'Data Quality Checks';
        PRINT 'Step 1: ' + @step_name + ' - Started';
        
        EXEC etl.sp_data_quality_check 'patients_raw';
        EXEC etl.sp_data_quality_check 'doctors_raw';
        EXEC etl.sp_data_quality_check 'admissions_raw';
        
        PRINT 'Step 1: ' + @step_name + ' - Completed';
        
        -- Step 2: Load Dimensions (SCD Type 2)
        SET @step_name = 'Dimension Loading';
        PRINT 'Step 2: ' + @step_name + ' - Started';
        
        EXEC etl.sp_load_patient_dimension;
        EXEC etl.sp_load_doctor_dimension;
        
        PRINT 'Step 2: ' + @step_name + ' - Completed';
        
        -- Step 3: Load Facts (Incremental)
        SET @step_name = 'Fact Loading';
        PRINT 'Step 3: ' + @step_name + ' - Started';
        
        EXEC etl.sp_load_admission_facts;
        EXEC etl.sp_load_procedure_facts;
        
        PRINT 'Step 3: ' + @step_name + ' - Completed';
        
        -- Step 4: Populate Analytics
        SET @step_name = 'Analytics Population';
        PRINT 'Step 4: ' + @step_name + ' - Started';
        
        EXEC analytics.sp_populate_daily_metrics;
        EXEC analytics.sp_populate_department_performance;
        
        PRINT 'Step 4: ' + @step_name + ' - Completed';
        
        PRINT '=================================================';
        PRINT 'Full ETL Pipeline completed successfully!';
        PRINT 'Total execution time: ' + CAST(DATEDIFF(SECOND, @start_time, GETDATE()) AS VARCHAR) + ' seconds';
        
    END TRY
    BEGIN CATCH
        SET @error_message = 'ETL Pipeline failed at step: ' + @step_name + '. Error: ' + ERROR_MESSAGE();
        PRINT @error_message;
        
        -- Log error to audit table
        INSERT INTO etl.data_quality_audit (table_name, check_name, check_result, record_count, error_details)
        VALUES ('ETL_PIPELINE', @step_name, 'FAILED', 0, @error_message);
        
        THROW;
    END CATCH
END;
GO