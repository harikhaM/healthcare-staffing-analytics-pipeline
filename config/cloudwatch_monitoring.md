# CloudWatch Monitoring

Amazon CloudWatch is used to monitor the ingestion and transformation pipeline.

---

## Lambda Monitoring

Lambda execution logs are automatically stored in CloudWatch.

Log group:

/aws/lambda/healthcare_gdrive_ingestion_lambda

Logs capture:

- ingestion start
- Google Drive authentication
- file download
- S3 upload status
- Glue job trigger

---

## Glue Job Monitoring

Glue job logs are also stored in CloudWatch.

Log groups:

/aws-glue/jobs/error
/aws-glue/jobs/output

Logs capture:

- ETL execution
- schema errors
- transformation errors
- partition write status

---

## Debugging

CloudWatch logs are used to debug issues such as:

- missing source files
- authentication errors
- ETL transformation failures
- S3 write issues