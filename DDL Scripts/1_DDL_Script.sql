/*
1. Database setup and Schema Creation

1.1 Create Database and Schema Structure
	- 4 Schemas - staging, dw, etl, analytics

1.2 Staging Tables (Raw Data Layers)
	- staging.patients_raw
	- staging.doctors_raw
	- staging.admissions_raw
	- staging.procedures_raw

1.3 Data Warehouse Dimension tables -scd2
	- 
1.4 Create Fact Tables DDL
1.5 Control and Audit Tables
*/

-- 1.1 Create Database and Schema Structure
CREATE DATABASE HospitalDataWarehouse;
GO

USE HospitalDataWarehouse;
GO

-- Create Schemas for Organization
CREATE SCHEMA staging;
GO
CREATE SCHEMA dw;
GO
CREATE SCHEMA etl;
GO
CREATE SCHEMA analytics;
GO

-- 1.2 Staging Tables (Raw Data Layers)
-- Staging table for patient data
CREATE TABLE staging.patients_raw (
    patient_id VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    phone VARCHAR(20),
    email VARCHAR(100),
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    insurance_provider VARCHAR(100),
    emergency_contact_name VARCHAR(100),
    emergency_contact_phone VARCHAR(20),
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    source_file VARCHAR(100)
);

-- Staging table for doctor data
CREATE TABLE staging.doctors_raw (
    doctor_id VARCHAR(20),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    specialization VARCHAR(100),
    department VARCHAR(100),
    hire_date DATE,
    salary DECIMAL(10,2),
    phone VARCHAR(20),
    email VARCHAR(100),
    license_number VARCHAR(50),
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    source_file VARCHAR(100)
);

-- Staging table for admission data
CREATE TABLE staging.admissions_raw (
    admission_id VARCHAR(20),
    patient_id VARCHAR(20),
    doctor_id VARCHAR(20),
    admission_date DATETIME2,
    discharge_date DATETIME2,
    room_number VARCHAR(10),
    admission_type VARCHAR(50),
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(500),
    treatment_cost DECIMAL(10,2),
    insurance_claim_amount DECIMAL(10,2),
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    source_file VARCHAR(100)
);

-- Staging table for procedures
CREATE TABLE staging.procedures_raw (
    procedure_id VARCHAR(20),
    admission_id VARCHAR(20),
    procedure_code VARCHAR(20),
    procedure_name VARCHAR(200),
    procedure_date DATETIME2,
    duration_minutes INT,
    cost DECIMAL(10,2),
    performing_doctor_id VARCHAR(20),
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    source_file VARCHAR(100)
);

-- 1.3 Data Warehouse Dimension tables -scd2
-- Patient dimension with SCD Type 2
CREATE TABLE dw.dim_patient (
    patient_key INT IDENTITY(1,1) PRIMARY KEY,
    patient_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(10),
    phone VARCHAR(20),
    email VARCHAR(100),
    address_line1 VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(10),
    insurance_provider VARCHAR(100),
    emergency_contact_name VARCHAR(100),
    emergency_contact_phone VARCHAR(20),
    -- SCD Type 2 columns
    effective_start_date DATETIME2 NOT NULL,
    effective_end_date DATETIME2,
    is_current BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()

);

-- Doctor dimension with SCD Type 2
CREATE TABLE dw.dim_doctor (
    doctor_key INT IDENTITY(1,1) PRIMARY KEY,
    doctor_id VARCHAR(20) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    specialization VARCHAR(100),
    department VARCHAR(100),
    hire_date DATE,
    salary DECIMAL(10,2),
    phone VARCHAR(20),
    email VARCHAR(100),
    license_number VARCHAR(50),
    -- SCD Type 2 columns
    effective_start_date DATETIME2 NOT NULL,
    effective_end_date DATETIME2,
    is_current BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- Date dimension
CREATE TABLE dw.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day_name VARCHAR(10),
    day_of_week INT,
    day_of_month INT,	
    day_of_year INT,
    week_of_year INT,
    month_name VARCHAR(15),
    month_of_year INT,
    quarter INT,
    year INT,
    is_weekend BIT
);

-- Time dimension
CREATE TABLE dw.dim_time (
    time_key INT PRIMARY KEY,
    full_time TIME,
    hour INT,
    minute INT,
    hour_description VARCHAR(20),
    shift VARCHAR(20)
);

-- 1.4 Create Fact Tables DDL
-- Admission fact table
CREATE TABLE dw.fact_admission (
    admission_key INT IDENTITY(1,1) PRIMARY KEY,
    admission_id VARCHAR(20) NOT NULL,
    patient_key INT,
    doctor_key INT,
    admission_date_key INT,
    admission_time_key INT,
    discharge_date_key INT,
    discharge_time_key INT,
    room_number VARCHAR(10),
    admission_type VARCHAR(50),
    diagnosis_code VARCHAR(20),
    diagnosis_description VARCHAR(500),
    treatment_cost DECIMAL(10,2),
    insurance_claim_amount DECIMAL(10,2),
    length_of_stay_days INT,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (patient_key) REFERENCES dw.dim_patient(patient_key),
    FOREIGN KEY (doctor_key) REFERENCES dw.dim_doctor(doctor_key),
    FOREIGN KEY (admission_date_key) REFERENCES dw.dim_date(date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dw.dim_date(date_key),
    FOREIGN KEY (admission_time_key) REFERENCES dw.dim_time(time_key),
    FOREIGN KEY (discharge_time_key) REFERENCES dw.dim_time(time_key)
);

-- Procedure fact table
CREATE TABLE dw.fact_procedure (
    procedure_key INT IDENTITY(1,1) PRIMARY KEY,
    procedure_id VARCHAR(20) NOT NULL,
    admission_key INT,
    procedure_date_key INT,
    procedure_time_key INT,
    performing_doctor_key INT,
    procedure_code VARCHAR(20),
    procedure_name VARCHAR(200),
    duration_minutes INT,
    cost DECIMAL(10,2),
    created_date DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (admission_key) REFERENCES dw.fact_admission(admission_key),
    FOREIGN KEY (procedure_date_key) REFERENCES dw.dim_date(date_key),
    FOREIGN KEY (procedure_time_key) REFERENCES dw.dim_time(time_key),
    FOREIGN KEY (performing_doctor_key) REFERENCES dw.dim_doctor(doctor_key)
);

-- ETL control table for incremental loading
CREATE TABLE etl.load_control (
    table_name VARCHAR(100) PRIMARY KEY,
    last_load_date DATETIME2,
    last_successful_load DATETIME2,
    load_status VARCHAR(20),
    records_loaded INT,
    created_date DATETIME2 DEFAULT GETDATE(),
    updated_date DATETIME2 DEFAULT GETDATE()
);

-- 1.5 Control and Audit Tables
-- Data quality audit table
CREATE TABLE etl.data_quality_audit (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    check_result VARCHAR(20),
    record_count INT,
    error_details VARCHAR(1000),
    check_date DATETIME2 DEFAULT GETDATE()
);

-- Initialize control table
INSERT INTO etl.load_control (table_name, last_load_date, last_successful_load, load_status)
VALUES 
    ('patients', '1900-01-01', '1900-01-01', 'READY'),
    ('doctors', '1900-01-01', '1900-01-01', 'READY'),
    ('admissions', '1900-01-01', '1900-01-01', 'READY'),
    ('procedures', '1900-01-01', '1900-01-01', 'READY');
