/*
Extra Code Snippets for rechecking
*/

-- Step 7: Verify data load
SELECT 'Patients' as table_name, COUNT(*) as record_count FROM dw.dim_patient
UNION ALL
SELECT 'Doctors', COUNT(*) FROM dw.dim_doctor
UNION ALL
SELECT 'Admissions', COUNT(*) FROM dw.fact_admission
UNION ALL
SELECT 'Procedures', COUNT(*) FROM dw.fact_procedure;

-- 7.2 Incremental Load Testing
-- Test incremental loading by adding new data
INSERT INTO staging.patients_raw (patient_id, first_name, last_name, date_of_birth, gender, phone, email, address_line1, city, state, zip_code, insurance_provider, emergency_contact_name, emergency_contact_phone)
VALUES ('PAT999999', 'Test', 'Patient', '1990-01-01', 'Male', '+1-555-555-5555', 'test.patient@email.com', '123 Test St', 'Test City', 'TX', '12345', 'Test Insurance', 'Emergency Contact', '+1-555-555-5556');

-- Update existing patient (SCD Type 2 test)
UPDATE staging.patients_raw 
SET phone = '+1-555-999-9999', email = 'updated.email@email.com'
WHERE patient_id = 'PAT000001';

-- Run ETL pipeline
EXEC etl.sp_run_full_etl_pipeline;

-- Verify SCD Type 2 implementation
SELECT patient_id, phone, email, effective_start_date, effective_end_date, is_current
FROM dw.dim_patient 
WHERE patient_id = 'PAT000001'
ORDER BY effective_start_date;

exec etl.sp_load_patient_dimension
exec etl.sp_load_doctor_dimension
exec etl.sp_load_admission_facts
exec etl.sp_load_procedure_facts

select top 5 * from staging.patients_raw;
select top 5 * from dw.dim_patient;

select top 5 * from staging.doctors_raw;
select top 5 * from dw.dim_doctor;

select top 5 * from dw.fact_admission;
select top 5 * from dw.fact_procedure;

