/*
4. Analytics Layer
4.1 Create Analytics Tables
4.2 Analytics Population Procedures
*/

-- 4.1 Create Analytics Tables

-- Daily hospital metrics
CREATE TABLE analytics.daily_hospital_metrics (
    metric_date DATE PRIMARY KEY,
    total_admissions INT,
    total_discharges INT,
    current_occupancy INT,
    average_length_of_stay DECIMAL(5,2),
    total_revenue DECIMAL(15,2),
    total_procedures INT,
    emergency_admissions INT,
    elective_admissions INT,
    created_date DATETIME2 DEFAULT GETDATE()
);

-- Department performance metrics
CREATE TABLE analytics.department_performance (
    department VARCHAR(100),
    metric_month DATE,
    total_admissions INT,
    average_length_of_stay DECIMAL(5,2),
    total_revenue DECIMAL(15,2),
    patient_satisfaction_score DECIMAL(3,2),
    readmission_rate DECIMAL(5,2),
    created_date DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (department, metric_month)
);

-- Doctor productivity metrics
CREATE TABLE analytics.doctor_productivity (
    doctor_key INT,
    metric_month DATE,
    total_patients INT,
    total_procedures INT,
    total_revenue DECIMAL(15,2),
    average_procedure_duration DECIMAL(10,2),
    patient_satisfaction_score DECIMAL(3,2),
    created_date DATETIME2 DEFAULT GETDATE(),
    PRIMARY KEY (doctor_key, metric_month),
    FOREIGN KEY (doctor_key) REFERENCES dw.dim_doctor(doctor_key)
);


-- 4.2 Analytics Population Procedures
GO
CREATE PROCEDURE analytics.sp_populate_daily_metrics
    @target_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @target_date IS NULL
        SET @target_date = CAST(GETDATE() AS DATE);
    
    -- Delete existing data for the date
    DELETE FROM analytics.daily_hospital_metrics WHERE metric_date = @target_date;
    
    -- Insert daily metrics
    INSERT INTO analytics.daily_hospital_metrics (
        metric_date, total_admissions, total_discharges, current_occupancy,
        average_length_of_stay, total_revenue, total_procedures,
        emergency_admissions, elective_admissions
    )
    SELECT 
        @target_date as metric_date,
        COUNT(CASE WHEN CAST(fa.created_date AS DATE) = @target_date THEN 1 END) as total_admissions,
        COUNT(CASE WHEN fa.discharge_date_key = CAST(FORMAT(@target_date, 'yyyyMMdd') AS INT) THEN 1 END) as total_discharges,
        COUNT(CASE WHEN fa.discharge_date_key IS NULL THEN 1 END) as current_occupancy,
        AVG(CAST(fa.length_of_stay_days AS DECIMAL(5,2))) as average_length_of_stay,
        SUM(fa.treatment_cost) as total_revenue,
        COUNT(fp.procedure_key) as total_procedures,
        COUNT(CASE WHEN fa.admission_type = 'Emergency' AND CAST(fa.created_date AS DATE) = @target_date THEN 1 END) as emergency_admissions,
        COUNT(CASE WHEN fa.admission_type = 'Elective' AND CAST(fa.created_date AS DATE) = @target_date THEN 1 END) as elective_admissions
    FROM dw.fact_admission fa
    LEFT JOIN dw.fact_procedure fp ON fa.admission_key = fp.admission_key
    WHERE fa.admission_date_key <= CAST(FORMAT(@target_date, 'yyyyMMdd') AS INT);
    
    PRINT 'Daily metrics populated for date: ' + CAST(@target_date AS VARCHAR);
END;
GO
CREATE PROCEDURE analytics.sp_populate_department_performance
    @target_month DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @target_month IS NULL
        SET @target_month = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0);
    
    DELETE FROM analytics.department_performance WHERE metric_month = @target_month;
    
    INSERT INTO analytics.department_performance (
        department, metric_month, total_admissions, average_length_of_stay,
        total_revenue, patient_satisfaction_score, readmission_rate
    )
    SELECT 
        dd.department,
        @target_month as metric_month,
        COUNT(fa.admission_key) as total_admissions,
        AVG(CAST(fa.length_of_stay_days AS DECIMAL(5,2))) as average_length_of_stay,
        SUM(fa.treatment_cost) as total_revenue,
        -- Simulated satisfaction score (in real scenario, this would come from patient surveys)
        CASE 
            WHEN AVG(CAST(fa.length_of_stay_days AS DECIMAL(5,2))) < 3 THEN 4.5
            WHEN AVG(CAST(fa.length_of_stay_days AS DECIMAL(5,2))) < 7 THEN 4.2
            ELSE 3.8
        END as patient_satisfaction_score,
        -- Simulated readmission rate
        CAST((COUNT(fa.admission_key) * 0.05) AS DECIMAL(5,2)) as readmission_rate
    FROM dw.fact_admission fa
    INNER JOIN dw.dim_doctor dd ON fa.doctor_key = dd.doctor_key AND dd.is_current = 1
    INNER JOIN dw.dim_date dt ON fa.admission_date_key = dt.date_key
    WHERE dt.year = YEAR(@target_month) AND dt.month_of_year = MONTH(@target_month)
    GROUP BY dd.department;
    
    PRINT 'Department performance metrics populated for month: ' + CAST(@target_month AS VARCHAR);
END;
GO

select * from analytics.doctor_productivity 