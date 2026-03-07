CREATE OR REPLACE VIEW healthcare_processed_db.v_fact_staffing_enriched AS
SELECT
    f.ccn,
    f.work_date,
    f.year_month,
    d.provider_name,
    d.facility_state,
    d.certified_beds,
    f.mds_census,
    f.hrs_rn,
    f.hrs_lpn,
    f.hrs_cna,
    f.total_nurse_hours,
    f.nurse_to_patient_ratio,
    f.bed_utilization_rate
FROM healthcare_processed_db.fact_staffing_daily f
LEFT JOIN healthcare_processed_db.dim_facility d
    ON f.ccn = d.ccn;

CREATE OR REPLACE VIEW healthcare_processed_db.v_monthly_staffing_kpis AS
SELECT
    year_month,
    AVG(nurse_to_patient_ratio) AS avg_nurse_to_patient_ratio,
    AVG(bed_utilization_rate) AS avg_bed_utilization_rate,
    SUM(total_nurse_hours) AS total_nurse_hours,
    AVG(mds_census) AS avg_mds_census
FROM healthcare_processed_db.v_fact_staffing_enriched
GROUP BY year_month;

CREATE OR REPLACE VIEW healthcare_processed_db.v_top_facilities_utilization AS
SELECT
    provider_name,
    ccn,
    AVG(bed_utilization_rate) AS avg_bed_utilization_rate,
    AVG(nurse_to_patient_ratio) AS avg_nurse_to_patient_ratio,
    SUM(total_nurse_hours) AS total_nurse_hours
FROM healthcare_processed_db.v_fact_staffing_enriched
GROUP BY provider_name, ccn;