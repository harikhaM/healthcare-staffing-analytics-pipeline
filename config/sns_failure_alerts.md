# Glue Failure Alerts (SNS + EventBridge)

Automated alerts are configured for Glue job failures.

---

## Alert Flow

Glue Job Failure
     ↓
EventBridge Rule
     ↓
SNS Topic
     ↓
Email Notification

---

## SNS Topic

Topic name:

healthcare-glue-failure-alerts

---

## EventBridge Rule

Rule name:

glue-job-failure-rule

Event source:

AWS Glue

Trigger states:

FAILED
TIMEOUT

---

## Notification

When the Glue job fails, an email alert is sent to the configured subscriber.

This enables quick troubleshooting of pipeline issues.