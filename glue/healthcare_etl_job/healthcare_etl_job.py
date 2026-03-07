import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    lpad,
    to_date,
    date_format,
    when,
    lit
)
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_DB = "healthcare_raw_db"
PROCESSED_PATH = "s3://healthcare-metrics-hmanthena/processed/"


# -----------------------------
# Read raw catalog tables
# -----------------------------
pbj_df = glueContext.create_dynamic_frame.from_catalog(
    database=RAW_DB,
    table_name="raw_pbj"
).toDF()

provider_df = glueContext.create_dynamic_frame.from_catalog(
    database=RAW_DB,
    table_name="raw_provider"
).toDF()

quality_df = glueContext.create_dynamic_frame.from_catalog(
    database=RAW_DB,
    table_name="raw_quality"
).toDF()


# -----------------------------
# Standardize PBJ dataset
# -----------------------------
pbj_df = pbj_df.withColumn("ccn", lpad(col("provnum").cast("string"), 6, "0"))

pbj_df = pbj_df.withColumn(
    "work_date",
    to_date(col("workdate"), "MM/dd/yyyy")
)

pbj_df = pbj_df.withColumn(
    "total_nurse_hours",
    (
        when(col("hrs_rn").isNull(), lit(0)).otherwise(col("hrs_rn")) +
        when(col("hrs_lpn").isNull(), lit(0)).otherwise(col("hrs_lpn")) +
        when(col("hrs_cna").isNull(), lit(0)).otherwise(col("hrs_cna"))
    )
)

pbj_df = pbj_df.withColumn(
    "mds_census",
    when(col("mdscensus").isNull(), lit(0)).otherwise(col("mdscensus"))
)

pbj_df = pbj_df.withColumn(
    "nurse_to_patient_ratio",
    when(col("mds_census") > 0, col("total_nurse_hours") / col("mds_census")).otherwise(lit(None))
)

pbj_df = pbj_df.withColumn(
    "year_month",
    date_format(col("work_date"), "yyyy-MM")
)


# -----------------------------
# Standardize Provider dataset
# -----------------------------
provider_df = provider_df.withColumnRenamed("cms certification number (ccn)", "ccn")

# rename a few common columns safely only if they exist
provider_columns = provider_df.columns
if "provider name" in provider_columns:
    provider_df = provider_df.withColumnRenamed("provider name", "provider_name")
if "provider state" in provider_columns:
    provider_df = provider_df.withColumnRenamed("provider state", "facility_state")
if "number of certified beds" in provider_columns:
    provider_df = provider_df.withColumnRenamed("number of certified beds", "certified_beds")


# -----------------------------
# Join PBJ with Provider
# -----------------------------
fact_staffing_daily = pbj_df.join(
    provider_df,
    on="ccn",
    how="left"
)

if "certified_beds" in fact_staffing_daily.columns:
    fact_staffing_daily = fact_staffing_daily.withColumn(
        "bed_utilization_rate",
        when(
            col("certified_beds").cast("double") > 0,
            col("mds_census") / col("certified_beds").cast("double")
        ).otherwise(lit(None))
    )
else:
    fact_staffing_daily = fact_staffing_daily.withColumn(
        "bed_utilization_rate",
        lit(None)
    )


# -----------------------------
# Build dimension table
# -----------------------------
dim_facility_cols = [c for c in [
    "ccn", "provider_name", "facility_state", "certified_beds"
] if c in fact_staffing_daily.columns]

dim_facility = fact_staffing_daily.select(*dim_facility_cols).dropDuplicates(["ccn"])


# -----------------------------
# Write outputs
# -----------------------------
fact_staffing_daily.write.mode("overwrite").partitionBy("year_month").parquet(
    f"{PROCESSED_PATH}fact_staffing_daily/"
)

dim_facility.write.mode("overwrite").parquet(
    f"{PROCESSED_PATH}dim_facility/"
)

quality_df.write.mode("overwrite").parquet(
    f"{PROCESSED_PATH}quality_measures/"
)

print("Processed datasets written successfully to S3 processed layer")

job.commit()