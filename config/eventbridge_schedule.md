# EventBridge Scheduling Configuration

Amazon EventBridge is used to trigger the ingestion pipeline automatically.

The scheduled event invokes the AWS Lambda ingestion function.

---

## Schedule Configuration

Schedule name:

healthcare-ingestion-schedule

Trigger type:

Recurring schedule

Cron expression:

cron(0 5,17 * * ? *)

---

## Execution Time

The pipeline runs twice per day:

5:00 AM
5:00 PM

---

## Pipeline Flow

EventBridge
    ↓
Lambda Ingestion
    ↓
Google Drive file download
    ↓
Upload to S3 Raw Layer
    ↓
Trigger Glue ETL Job

---

## Target

Lambda function:

healthcare_gdrive_ingestion_lambda