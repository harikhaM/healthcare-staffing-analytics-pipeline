## Glue Job Name

healthcare_etl_job

## Suggested Job Type

Spark / PySpark

## IAM Role

AWSGlueServiceRole-healthcare

## Optional Job Parameters

--job-language python
--enable-continuous-cloudwatch-log true
--continuous-log-logGroup /aws-glue/jobs/logs-v2
--continuous-log-logStreamPrefix healthcare-etl
