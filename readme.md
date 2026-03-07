# Healthcare Staffing Analytics Platform

Project Description

This project implements an end-to-end serverless data engineering pipeline on AWS to analyze healthcare staffing and facility performance metrics. The pipeline ingests healthcare datasets from Google Drive, processes them through a scalable AWS data lake architecture, and enables analytical insights through Amazon Athena and Amazon QuickSight dashboards.

The solution automates data ingestion, transformation, and analytics using a fully serverless approach. Amazon EventBridge triggers an AWS Lambda ingestion function that securely downloads healthcare CSV files from Google Drive and loads them into an Amazon S3 raw data lake layer. AWS Glue Crawlers automatically detect schemas and register datasets in the AWS Glue Data Catalog, which serves as the centralized metadata repository.

Data transformation and KPI generation are performed using an AWS Glue PySpark ETL job. The ETL pipeline cleans and standardizes raw datasets, performs joins between provider and staffing data, and calculates key operational metrics such as:

Nurse-to-patient ratio
Total nurse staffing hours
Bed utilization rate
Patient census trends

The transformed datasets are written in Parquet format to the S3 processed layer, enabling efficient analytical querying. Amazon Athena is used as the SQL query layer to analyze processed data directly from the data lake, while Amazon QuickSight provides interactive dashboards for operational insights and visualization of healthcare staffing trends.

The project also includes monitoring and observability features. AWS CloudWatch captures logs for both Lambda ingestion and Glue ETL jobs, and EventBridge rules integrated with Amazon SNS provide automated alerts when pipeline failures occur.

Overall, this project demonstrates a modern cloud-native data platform architecture that leverages AWS serverless services to build a scalable, cost-efficient healthcare analytics pipeline.

## Architecture

Google Drive → EventBridge → Lambda → S3 Raw → Glue Crawler → Glue ETL → S3 Processed → Athena → QuickSight

## Technologies Used

Amazon EventBridge
AWS Lambda
Amazon S3 (Data Lake – Raw & Processed Layers)
AWS Glue Crawler & Glue Data Catalog
AWS Glue ETL (PySpark)
Amazon Athena
Amazon QuickSight
Amazon CloudWatch
Amazon SNS

## Key Features

- Automated ingestion from Google Drive
- Secure credential handling with AWS Secrets Manager
- Raw and processed data lake layers in Amazon S3
- AWS Glue crawler for schema discovery
- AWS Glue PySpark ETL for KPI calculations
- Analytics-ready Parquet output
- Athena SQL views for reporting
- CloudWatch logging and SNS failure alerts

## KPIs

- Nurse-to-patient ratio
- Total nurse hours
- Bed utilization rate
- Facility-level staffing trends
- Monthly staffing trends

## Repository Structure

- `lambda/` → ingestion code
- `glue/` → ETL job code
- `sql/` → Athena queries and views
- `docs/` → project architecture documents
- `screenshots/` → dashboard and AWS screenshots

## Security

This repository does not include real credentials, secrets, or private identifiers.
