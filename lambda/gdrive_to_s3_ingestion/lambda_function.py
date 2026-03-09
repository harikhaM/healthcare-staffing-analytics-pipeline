import json
import io
import logging
import traceback
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# =========================
# CONFIGURATION
# =========================
S3_BUCKET = "healthcare-metrics-hmanthena"
DRIVE_FOLDER_ID = "YOUR_GOOGLE_DRIVE_FOLDER_ID"
SECRET_NAME = "gdrive_service_account_json"
REGION = "us-east-1"
GLUE_JOB_NAME = "healthcare_etl_job"

CONTROL_FILE_KEY = "control/google_drive_ingestion_tracker.json"

FILE_TO_S3_PREFIX = {
    "PBJ_Daily_Nurse_Staffing_Q2_2024.csv": "raw/pbj/",
    "NH_ProviderInfo_Oct2024.csv": "raw/provider/",
    "NH_QualityMsr_Claims_Oct2024.csv": "raw/quality/"
}

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# =========================
# LOGGING SETUP
# =========================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# =========================
# AWS CLIENTS
# =========================
secrets_client = boto3.client("secretsmanager", region_name=REGION)
s3_client = boto3.client("s3")
glue_client = boto3.client("glue", region_name=REGION)


# =========================
# GET GOOGLE DRIVE SERVICE
# =========================
def get_drive_service():
    try:
        logger.info("Fetching Google service account secret from Secrets Manager")
        secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret_string = secret_response["SecretString"]
        service_account_info = json.loads(secret_string)

        logger.info("Creating Google Drive credentials")
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=SCOPES
        )

        logger.info("Building Google Drive service")
        drive_service = build("drive", "v3", credentials=credentials)

        return drive_service

    except Exception as e:
        logger.error(f"Failed to create Google Drive service: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# READ CONTROL FILE
# =========================
def read_control_file():
    try:
        logger.info(f"Reading control file from s3://{S3_BUCKET}/{CONTROL_FILE_KEY}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=CONTROL_FILE_KEY)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code in ["NoSuchKey", "NoSuchBucket", "404"]:
            logger.warning("Control file not found. Assuming initial run.")
            return {}

        logger.error(f"Error reading control file: {str(e)}")
        logger.error(traceback.format_exc())
        raise

    except Exception as e:
        logger.error(f"Unexpected error reading control file: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# WRITE CONTROL FILE
# =========================
def write_control_file(control_data):
    try:
        logger.info(f"Writing updated control file to s3://{S3_BUCKET}/{CONTROL_FILE_KEY}")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=CONTROL_FILE_KEY,
            Body=json.dumps(control_data, indent=2).encode("utf-8")
        )

    except Exception as e:
        logger.error(f"Failed to write control file: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# LIST FILES IN GOOGLE DRIVE
# =========================
def list_drive_files(drive_service):
    try:
        logger.info("Listing files from Google Drive folder")
        query = f"'{DRIVE_FOLDER_ID}' in parents and trashed=false"

        results = drive_service.files().list(
            q=query,
            fields="files(id,name,modifiedTime)"
        ).execute()

        files = results.get("files", [])
        logger.info(f"Found {len(files)} files in Google Drive folder")

        return files

    except Exception as e:
        logger.error(f"Failed to list files from Google Drive: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# DOWNLOAD FILE FROM GOOGLE DRIVE
# =========================
def download_file(drive_service, file_id, filename):
    try:
        logger.info(f"Downloading file from Google Drive: {filename}")
        request = drive_service.files().get_media(fileId=file_id)

        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        file_stream.seek(0)
        logger.info(f"Download completed for file: {filename}")

        return file_stream.read()

    except Exception as e:
        logger.error(f"Failed to download file {filename}: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# UPLOAD FILE TO S3
# =========================
def upload_to_s3(content, s3_key, filename):
    try:
        logger.info(f"Uploading {filename} to s3://{S3_BUCKET}/{s3_key}")
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=content
        )
        logger.info(f"Successfully uploaded {filename}")

    except Exception as e:
        logger.error(f"Failed to upload {filename} to S3: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# TRIGGER GLUE JOB
# =========================
def trigger_glue_job():
    try:
        logger.info(f"Triggering Glue job: {GLUE_JOB_NAME}")
        response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response["JobRunId"]
        logger.info(f"Glue job started successfully. JobRunId: {job_run_id}")
        return job_run_id

    except Exception as e:
        logger.error(f"Failed to trigger Glue job: {str(e)}")
        logger.error(traceback.format_exc())
        raise


# =========================
# MAIN LAMBDA HANDLER
# =========================
def lambda_handler(event, context):
    try:
        logger.info("===== Lambda ingestion started =====")
        logger.info(f"Lambda request id: {context.aws_request_id}")

        drive_service = get_drive_service()
        control_data = read_control_file()
        drive_files = list_drive_files(drive_service)

        # Convert list to dict for faster lookup
        drive_file_map = {
            f["name"]: {
                "id": f["id"],
                "modifiedTime": f["modifiedTime"]
            }
            for f in drive_files
        }

        uploaded_files = []
        missing_files = []
        skipped_files = []

        load_date = datetime.utcnow().strftime("%Y-%m-%d")

        for filename, prefix in FILE_TO_S3_PREFIX.items():
            logger.info(f"Processing file: {filename}")

            if filename not in drive_file_map:
                logger.warning(f"File not found in Google Drive: {filename}")
                missing_files.append(filename)
                continue

            file_id = drive_file_map[filename]["id"]
            modified_time = drive_file_map[filename]["modifiedTime"]

            # Incremental check
            last_processed_time = control_data.get(filename)

            if last_processed_time == modified_time:
                logger.info(f"Skipping unchanged file: {filename}")
                skipped_files.append(filename)
                continue

            logger.info(f"New or updated file detected: {filename}")

            content = download_file(drive_service, file_id, filename)

            s3_key = f"{prefix}load_date={load_date}/{filename}"
            upload_to_s3(content, s3_key, filename)

            uploaded_files.append(s3_key)

            # Update checkpoint only after successful upload
            control_data[filename] = modified_time

        # Write updated control file
        if uploaded_files:
            write_control_file(control_data)

        glue_job_run_id = None
        if uploaded_files:
            logger.info("New files uploaded successfully. Triggering Glue job.")
            glue_job_run_id = trigger_glue_job()
        else:
            logger.info("No new files uploaded. Glue job will not be triggered.")

        logger.info("===== Lambda ingestion completed successfully =====")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "success",
                "uploaded_files": uploaded_files,
                "skipped_files": skipped_files,
                "missing_files": missing_files,
                "glue_job_triggered": bool(glue_job_run_id),
                "glue_job_run_id": glue_job_run_id
            })
        }

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        logger.error(traceback.format_exc())

        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "failed",
                "error": str(e)
            })
        }