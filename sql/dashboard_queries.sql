-- Monthly staffing trend
SELECT
    year_month,
    AVG(nurse_to_patient_ratio) AS avg_ratio
FROM healthcare_processed_db.fact_staffing_daily
GROUP BY year_month
ORDER BY year_month;

-- State-level staffing analysis
SELECT
    d.facility_state,
    AVG(f.nurse_to_patient_ratio) AS avg_ratio
FROM healthcare_processed_db.fact_staffing_daily f
JOIN healthcare_processed_db.dim_facility d
    ON f.ccn = d.ccn
GROUP BY d.facility_state
ORDER BY avg_ratio DESC;

-- Top hospitals by utilization
SELECT
    provider_name,
    AVG(bed_utilization_rate) AS avg_utilization
FROM healthcare_processed_db.v_fact_staffing_enriched
GROUP BY provider_name
ORDER BY avg_utilization DESC
LIMIT 10;

-- Total nurse hours by hospital
SELECT
    provider_name,
    SUM(total_nurse_hours) AS total_nurse_hours
FROM healthcare_processed_db.v_fact_staffing_enriched
GROUP BY provider_name
ORDER BY total_nurse_hours DESC
LIMIT 10;