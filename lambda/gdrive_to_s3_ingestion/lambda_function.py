import os
import io
import json
import uuid
import boto3
from datetime import datetime, timezone

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
glue_client = boto3.client("glue")


BUCKET_NAME = os.environ.get("BUCKET_NAME", "healthcare-metrics-hmanthena")
SECRET_NAME = os.environ.get("SECRET_NAME", "gdrive_service_account_json")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "healthcare_etl_job")
GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "YOUR_GOOGLE_DRIVE_FOLDER_ID")

# Map source filenames to S3 raw folders
FILE_TO_PREFIX = {
    "PBJ_Daily_Nurse_Staffing_Q2_2024.csv": "raw/pbj",
    "NH_ProviderInfo_Oct2024.csv": "raw/provider",
    "NH_QualityMsr_Claims_Oct2024.csv": "raw/quality",
}


def get_google_credentials(secret_name: str):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_string = response["SecretString"]
    service_account_info = json.loads(secret_string)

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return credentials


def get_drive_service():
    credentials = get_google_credentials(SECRET_NAME)
    return build("drive", "v3", credentials=credentials)


def list_files_in_folder(drive_service, folder_id: str):
    query = f"'{folder_id}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query,
        fields="files(id, name, mimeType)"
    ).execute()
    return results.get("files", [])


def download_file_from_drive(drive_service, file_id: str) -> bytes:
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    fh.seek(0)
    return fh.read()


def upload_to_s3(file_bytes: bytes, bucket: str, key: str):
    s3.put_object(Bucket=bucket, Key=key, Body=file_bytes)
    print(f"Uploaded file to s3://{bucket}/{key}")


def write_success_marker(bucket: str, load_date: str, run_id: str, uploaded_files: list):
    marker_key = f"raw/_manifest/load_date={load_date}/_SUCCESS.json"
    marker_body = {
        "status": "SUCCESS",
        "load_date": load_date,
        "run_id": run_id,
        "uploaded_files": uploaded_files,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }
    s3.put_object(
        Bucket=bucket,
        Key=marker_key,
        Body=json.dumps(marker_body, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"Success marker written to s3://{bucket}/{marker_key}")


def trigger_glue_job():
    response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
    print(f"Glue job started: {GLUE_JOB_NAME}, RunId: {response['JobRunId']}")
    return response["JobRunId"]


def lambda_handler(event, context):
    print("Starting Google Drive to S3 ingestion pipeline")

    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = str(uuid.uuid4())

    drive_service = get_drive_service()
    drive_files = list_files_in_folder(drive_service, GDRIVE_FOLDER_ID)

    if not drive_files:
        raise Exception("No files found in Google Drive folder")

    uploaded_files = []

    for file in drive_files:
        file_name = file["name"]
        file_id = file["id"]

        if file_name not in FILE_TO_PREFIX:
            print(f"Skipping unexpected file: {file_name}")
            continue

        prefix = FILE_TO_PREFIX[file_name]
        s3_key = f"{prefix}/load_date={load_date}/run_id={run_id}/{file_name}"

        print(f"Downloading file from Google Drive: {file_name}")
        file_bytes = download_file_from_drive(drive_service, file_id)

        print(f"Uploading file to S3: {s3_key}")
        upload_to_s3(file_bytes, BUCKET_NAME, s3_key)

        uploaded_files.append({
            "file_name": file_name,
            "s3_key": s3_key
        })

    if not uploaded_files:
        raise Exception("No expected source files were uploaded")

    write_success_marker(BUCKET_NAME, load_date, run_id, uploaded_files)

    glue_run_id = trigger_glue_job()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ingestion completed successfully",
            "load_date": load_date,
            "run_id": run_id,
            "uploaded_file_count": len(uploaded_files),
            "glue_run_id": glue_run_id
        })
    }