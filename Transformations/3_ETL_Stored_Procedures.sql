/*
3. ETL Stored Procedures

3.1 Data Quality Check Procedures
3.2 SCD Type 2 Implementation for Patients
3.3 SCD Type 2 Implementation for Doctors
3.4 Incremental Fact Table Loading
3.5 Procedure Facts Loading
*/

-- 3.1 Data Quality Check Procedures - Pore procedure m sirf NULL check kia isne
CREATE PROCEDURE etl.sp_data_quality_check
    @table_name VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @record_count INT;
    DECLARE @error_details VARCHAR(1000) = '';
    DECLARE @check_result VARCHAR(20) = 'PASSED';
    
    -- Check for null values in key columns
    IF @table_name = 'patients_raw'
    BEGIN
        SELECT @record_count = COUNT(*) 
        FROM staging.patients_raw 
        WHERE patient_id IS NULL OR first_name IS NULL OR last_name IS NULL;
        
        IF @record_count > 0
        BEGIN
            SET @check_result = 'FAILED';
            SET @error_details = 'Found ' + CAST(@record_count AS VARCHAR) + ' records with null key fields';
        END
    END
    
    IF @table_name = 'doctors_raw'
    BEGIN
        SELECT @record_count = COUNT(*) 
        FROM staging.doctors_raw 
        WHERE doctor_id IS NULL OR first_name IS NULL OR last_name IS NULL OR specialization IS NULL;
        
        IF @record_count > 0
        BEGIN
            SET @check_result = 'FAILED';
            SET @error_details = 'Found ' + CAST(@record_count AS VARCHAR) + ' records with null key fields';
        END
    END
    
    IF @table_name = 'admissions_raw'
    BEGIN
        SELECT @record_count = COUNT(*) 
        FROM staging.admissions_raw 
        WHERE admission_id IS NULL OR patient_id IS NULL OR doctor_id IS NULL OR admission_date IS NULL;
        
        IF @record_count > 0
        BEGIN
            SET @check_result = 'FAILED';
            SET @error_details = 'Found ' + CAST(@record_count AS VARCHAR) + ' records with null key fields';
        END
    END
    
    -- Insert audit record
    INSERT INTO etl.data_quality_audit (table_name, check_name, check_result, record_count, error_details)
    VALUES (@table_name, 'Null Key Fields Check', @check_result, @record_count, @error_details);
    
    -- Return result
    SELECT @check_result as result, @error_details as details;
END;
GO

CREATE PROCEDURE etl.sp_load_patient_dimension
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @load_date DATETIME2 = GETDATE();
    DECLARE @records_processed INT = 0;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Update load control
        UPDATE etl.load_control 
        SET load_status = 'RUNNING', updated_date = @load_date
        WHERE table_name = 'patients';
        
        -- Handle new records (Type 1 insert)
        INSERT INTO dw.dim_patient (
            patient_id, first_name, last_name, date_of_birth, gender,
            phone, email, address_line1, city, state, zip_code,
            insurance_provider, emergency_contact_name, emergency_contact_phone,
            effective_start_date, effective_end_date, is_current
        )
        SELECT 
            s.patient_id, s.first_name, s.last_name, s.date_of_birth, s.gender,
            s.phone, s.email, s.address_line1, s.city, s.state, s.zip_code,
            s.insurance_provider, s.emergency_contact_name, s.emergency_contact_phone,
            @load_date, NULL, 1
        FROM staging.patients_raw s
        LEFT JOIN dw.dim_patient d ON s.patient_id = d.patient_id
        WHERE d.patient_id IS NULL;
        
        SET @records_processed = @@ROWCOUNT;
        
        -- Handle SCD Type 2 updates (expire old records and insert new versions)
        -- First, identify changed records
        WITH ChangedRecords AS (
            SELECT s.patient_id
            FROM staging.patients_raw s
            INNER JOIN dw.dim_patient d ON s.patient_id = d.patient_id AND d.is_current = 1
            WHERE s.phone != d.phone 
               OR s.email != d.email 
               OR s.address_line1 != d.address_line1 
               OR s.city != d.city 
               OR s.state != d.state 
               OR s.zip_code != d.zip_code
               OR s.insurance_provider != d.insurance_provider
               OR s.emergency_contact_name != d.emergency_contact_name
               OR s.emergency_contact_phone != d.emergency_contact_phone
        )
        -- Expire old records
        UPDATE dw.dim_patient 
        SET 
            effective_end_date = DATEADD(SECOND, -1, @load_date),
            is_current = 0,
            updated_date = @load_date
        FROM dw.dim_patient d
        INNER JOIN ChangedRecords c ON d.patient_id = c.patient_id
        WHERE d.is_current = 1;
        
        -- Insert new versions of changed records
        INSERT INTO dw.dim_patient (
            patient_id, first_name, last_name, date_of_birth, gender,
            phone, email, address_line1, city, state, zip_code,
            insurance_provider, emergency_contact_name, emergency_contact_phone,
            effective_start_date, effective_end_date, is_current
        )
        SELECT 
            s.patient_id, s.first_name, s.last_name, s.date_of_birth, s.gender,
            s.phone, s.email, s.address_line1, s.city, s.state, s.zip_code,
            s.insurance_provider, s.emergency_contact_name, s.emergency_contact_phone,
            @load_date, NULL, 1
        FROM staging.patients_raw s
        INNER JOIN dw.dim_patient d ON s.patient_id = d.patient_id AND d.effective_end_date IS NOT NULL
        WHERE d.updated_date = @load_date;
        
        SET @records_processed = @records_processed + @@ROWCOUNT;
        
        -- Update load control
        UPDATE etl.load_control 
        SET 
            last_load_date = @load_date,
            last_successful_load = @load_date,
            load_status = 'SUCCESS',
            records_loaded = @records_processed,
            updated_date = @load_date
        WHERE table_name = 'patients';
        
        COMMIT TRANSACTION;
        
        PRINT 'Patient dimension load completed successfully. Records processed: ' + CAST(@records_processed AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        UPDATE etl.load_control 
        SET load_status = 'FAILED', updated_date = @load_date
        WHERE table_name = 'patients';
        
        THROW;
    END CATCH
END;
GO

-- 3.3 SCD Type 2 Implementation for Doctors
CREATE PROCEDURE etl.sp_load_doctor_dimension
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @load_date DATETIME2 = GETDATE();
    DECLARE @records_processed INT = 0;
    
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Update load control
        UPDATE etl.load_control 
        SET load_status = 'RUNNING', updated_date = @load_date
        WHERE table_name = 'doctors';
        
        -- Handle new records
        INSERT INTO dw.dim_doctor (
            doctor_id, first_name, last_name, specialization, department,
            hire_date, salary, phone, email, license_number,
            effective_start_date, effective_end_date, is_current
        )
        SELECT 
            s.doctor_id, s.first_name, s.last_name, s.specialization, s.department,
            s.hire_date, s.salary, s.phone, s.email, s.license_number,
            @load_date, NULL, 1
        FROM staging.doctors_raw s
        LEFT JOIN dw.dim_doctor d ON s.doctor_id = d.doctor_id
        WHERE d.doctor_id IS NULL;
        
        SET @records_processed = @@ROWCOUNT;
        
/*        -- Handle SCD Type 2 updates for salary, department, phone, email changes
        WITH ChangedRecords AS (
            SELECT s.doctor_id
            FROM staging.doctors_raw s
            INNER JOIN dw.dim_doctor d ON s.doctor_id = d.doctor_id AND d.is_current = 1
            WHERE s.salary != d.salary 
               OR s.department != d.department 
               OR s.phone != d.phone 
               OR s.email != d.email
               OR s.specialization != d.specialization
        )
        -- Expire old records
        UPDATE dw.dim_doctor 
        SET 
            effective_end_date = DATEADD(SECOND, -1, @load_date),
            is_current = 0,
            updated_date = @load_date
        FROM dw.dim_doctor d
        INNER JOIN ChangedRecords c ON d.doctor_id = c.doctor_id
        WHERE d.is_current = 1;
        
        -- Insert new versions
        INSERT INTO dw.dim_doctor (
            doctor_id, first_name, last_name, specialization, department,
            hire_date, salary, phone, email, license_number,
            effective_start_date, effective_end_date, is_current
        )
        SELECT 
            s.doctor_id, s.first_name, s.last_name, s.specialization, s.department,
            s.hire_date, s.salary, s.phone, s.email, s.license_number,
            @load_date, NULL, 1
        FROM staging.doctors_raw s
        INNER JOIN dw.dim_doctor d ON s.doctor_id = d.doctor_id AND d.effective_end_date IS NOT NULL
        WHERE d.updated_date = @load_date;
        
        SET @records_processed = @records_processed + @@ROWCOUNT;
  */      
        -- Update load control
        UPDATE etl.load_control 
        SET 
            last_load_date = @load_date,
            last_successful_load = @load_date,
            load_status = 'SUCCESS',
            records_loaded = @records_processed,
            updated_date = @load_date
        WHERE table_name = 'doctors';
        
        COMMIT TRANSACTION;
        
        PRINT 'Doctor dimension load completed successfully. Records processed: ' + CAST(@records_processed AS VARCHAR);
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        
        UPDATE etl.load_control 
        SET load_status = 'FAILED', updated_date = @load_date
        WHERE table_name = 'doctors';
        
        THROW;
    END CATCH
END;
GO

-- 3.4 Incremental Fact Table Loading
CREATE PROCEDURE etl.sp_load_admission_facts
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @load_date DATETIME2 = GETDATE();
    DECLARE @last_load_date DATETIME2;
    DECLARE @records_processed INT = 0;

    -- Get last successful load date
    SELECT @last_load_date = last_successful_load
    FROM etl.load_control
    WHERE table_name = 'admissions';

	IF @last_load_date IS NULL
	    SET @last_load_date = '1900-01-01';

    BEGIN TRANSACTION;

    BEGIN TRY
        -- Update load control to running
        UPDATE etl.load_control
        SET load_status = 'RUNNING', updated_date = @load_date
        WHERE table_name = 'admissions';

        -- Incremental insert for new records
        INSERT INTO dw.fact_admission (
            admission_id, patient_key, doctor_key, admission_date_key, admission_time_key,
            discharge_date_key, discharge_time_key, room_number, admission_type,
            diagnosis_code, diagnosis_description, treatment_cost, insurance_claim_amount,
            length_of_stay_days, created_date
        )
        SELECT 
            s.admission_id,
            p.patient_key,
            d.doctor_key,
            CAST(FORMAT(s.admission_date, 'yyyyMMdd') AS INT) AS admission_date_key,
            (DATEPART(HOUR, s.admission_date)*100 + 
             CASE 
                WHEN DATEPART(MINUTE, s.admission_date) >= 45 THEN 45
                WHEN DATEPART(MINUTE, s.admission_date) >= 30 THEN 30
                WHEN DATEPART(MINUTE, s.admission_date) >= 15 THEN 15
                ELSE 0
             END) AS admission_time_key,
            CASE WHEN s.discharge_date IS NOT NULL 
                 THEN CAST(FORMAT(s.discharge_date, 'yyyyMMdd') AS INT)
                 ELSE NULL
            END AS discharge_date_key,
            CASE WHEN s.discharge_date IS NOT NULL
                 THEN (DATEPART(HOUR, s.discharge_date)*100 +
                      CASE 
                          WHEN DATEPART(MINUTE, s.discharge_date) >= 45 THEN 45
                          WHEN DATEPART(MINUTE, s.discharge_date) >= 30 THEN 30
                          WHEN DATEPART(MINUTE, s.discharge_date) >= 15 THEN 15
                          ELSE 0
                      END)
                 ELSE NULL
            END AS discharge_time_key,
            s.room_number,
            s.admission_type,
            s.diagnosis_code,
            s.diagnosis_description,
            s.treatment_cost,
            s.insurance_claim_amount,
            CASE WHEN s.discharge_date IS NOT NULL 
                 THEN DATEDIFF(DAY, s.admission_date, s.discharge_date)
                 ELSE NULL
            END AS length_of_stay_days,
            @load_date
        FROM staging.admissions_raw s
        INNER JOIN dw.dim_patient p ON s.patient_id = p.patient_id AND p.is_current = 1
        INNER JOIN dw.dim_doctor d ON s.doctor_id = d.doctor_id AND d.is_current = 1
        INNER JOIN dw.dim_date ad ON CAST(FORMAT(s.admission_date, 'yyyyMMdd') AS INT) = ad.date_key
        INNER JOIN dw.dim_time at ON (DATEPART(HOUR, s.admission_date)*100 + 
                                     CASE 
                                         WHEN DATEPART(MINUTE, s.admission_date) >= 45 THEN 45
                                         WHEN DATEPART(MINUTE, s.admission_date) >= 30 THEN 30
                                         WHEN DATEPART(MINUTE, s.admission_date) >= 15 THEN 15
                                         ELSE 0
                                     END) = at.time_key
        LEFT JOIN dw.fact_admission f ON s.admission_id = f.admission_id
        WHERE s.load_timestamp > @last_load_date
        AND f.admission_id IS NULL;

        SET @records_processed = @@ROWCOUNT;

        -- Update existing records if discharge info or cost changed
        UPDATE f
        SET 
            discharge_date_key = CASE WHEN s.discharge_date IS NOT NULL 
                                      THEN CAST(FORMAT(s.discharge_date, 'yyyyMMdd') AS INT)
                                      ELSE NULL END,
            discharge_time_key = CASE WHEN s.discharge_date IS NOT NULL
                                      THEN (DATEPART(HOUR, s.discharge_date)*100 +
                                           CASE 
                                               WHEN DATEPART(MINUTE, s.discharge_date) >= 45 THEN 45
                                               WHEN DATEPART(MINUTE, s.discharge_date) >= 30 THEN 30
                                               WHEN DATEPART(MINUTE, s.discharge_date) >= 15 THEN 15
                                               ELSE 0
                                           END)
                                      ELSE NULL END,
            length_of_stay_days = CASE WHEN s.discharge_date IS NOT NULL
                                       THEN DATEDIFF(DAY, s.admission_date, s.discharge_date)
                                       ELSE NULL END,
            treatment_cost = s.treatment_cost,
            insurance_claim_amount = s.insurance_claim_amount,
            updated_date = @load_date
        FROM dw.fact_admission f
        INNER JOIN staging.admissions_raw s ON f.admission_id = s.admission_id
        WHERE s.load_timestamp > @last_load_date;

        SET @records_processed = @records_processed + @@ROWCOUNT;

        -- Update load control to success
        UPDATE etl.load_control
        SET 
            last_load_date = @load_date,
            last_successful_load = @load_date,
            load_status = 'SUCCESS',
            records_loaded = @records_processed,
            updated_date = @load_date
        WHERE table_name = 'admissions';

        COMMIT TRANSACTION;

        PRINT 'Admission facts load completed successfully. Records processed: ' + CAST(@records_processed AS VARCHAR);

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        UPDATE etl.load_control
        SET load_status = 'FAILED', updated_date = @load_date
        WHERE table_name = 'admissions';

        THROW;
    END CATCH
END;
GO

-- 3.5 Procedure Facts Loading
CREATE PROCEDURE etl.sp_load_procedure_facts
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @load_date DATETIME2 = GETDATE();
    DECLARE @last_load_date DATETIME2;
    DECLARE @records_processed INT = 0;

    -- Get last successful load date
    SELECT @last_load_date = last_successful_load
    FROM etl.load_control
    WHERE table_name = 'procedures';

	--IF @last_load_date IS NULL
	  --  SET @last_load_date = '1900-01-01';

    BEGIN TRANSACTION;

    BEGIN TRY
        -- Update load control
        UPDATE etl.load_control
        SET load_status = 'RUNNING', updated_date = @load_date
        WHERE table_name = 'procedures';

        -- Insert new procedures
        INSERT INTO dw.fact_procedure (
            procedure_id, admission_key, procedure_date_key, procedure_time_key,
            performing_doctor_key, procedure_code, procedure_name,
            duration_minutes, cost, created_date
        )
        SELECT 
            s.procedure_id,
            a.admission_key,
            CAST(FORMAT(s.procedure_date, 'yyyyMMdd') AS INT) AS procedure_date_key,
            (DATEPART(HOUR, s.procedure_date)*100 + 
             CASE 
                 WHEN DATEPART(MINUTE, s.procedure_date) >= 45 THEN 45
                 WHEN DATEPART(MINUTE, s.procedure_date) >= 30 THEN 30
                 WHEN DATEPART(MINUTE, s.procedure_date) >= 15 THEN 15
                 ELSE 0
             END) AS procedure_time_key,
            d.doctor_key,
            s.procedure_code,
            s.procedure_name,
            s.duration_minutes,
            s.cost,
            @load_date
        FROM staging.procedures_raw s
        INNER JOIN dw.fact_admission a ON s.admission_id = a.admission_id
        INNER JOIN dw.dim_doctor d ON s.performing_doctor_id = d.doctor_id AND d.is_current = 1
        INNER JOIN dw.dim_date pd ON CAST(FORMAT(s.procedure_date, 'yyyyMMdd') AS INT) = pd.date_key
        INNER JOIN dw.dim_time pt ON (DATEPART(HOUR, s.procedure_date)*100 + 
                                      CASE 
                                          WHEN DATEPART(MINUTE, s.procedure_date) >= 45 THEN 45
                                          WHEN DATEPART(MINUTE, s.procedure_date) >= 30 THEN 30
                                          WHEN DATEPART(MINUTE, s.procedure_date) >= 15 THEN 15
                                          ELSE 0
                                      END) = pt.time_key
        LEFT JOIN dw.fact_procedure f ON s.procedure_id = f.procedure_id
        WHERE s.load_timestamp > @last_load_date
        AND f.procedure_id IS NULL;

        SET @records_processed = @@ROWCOUNT;

        -- Update load control to success
        UPDATE etl.load_control
        SET 
            last_load_date = @load_date,
            last_successful_load = @load_date,
            load_status = 'SUCCESS',
            records_loaded = @records_processed,
            updated_date = @load_date
        WHERE table_name = 'procedures';

        COMMIT TRANSACTION;

        PRINT 'Procedure facts load completed successfully. Records processed: ' + CAST(@records_processed AS VARCHAR);

    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        UPDATE etl.load_control
        SET load_status = 'FAILED', updated_date = @load_date
        WHERE table_name = 'procedures';

        THROW;
    END CATCH
END;
GO
