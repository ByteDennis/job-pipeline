# S3 Utilities for Data Validation Pipeline
# Handles S3 file operations including upload/download of parquet and JSON files

import os
import sys
import time
import threading
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger
import boto3
import requests
import urllib3
import s3fs
import awswrangler as aws

try:
    from upath import UPath
except ImportError:
    UPath = Path

# Constants
inWindows = os.name == 'nt'
SESSION = None
AWS_REGION = None


#>>> Check if S3 session is expired <<<#
def s3_is_expired(delta=0):
    if hasattr(SESSION, 'expire_time'):
        t_expire = SESSION.expire_time
        t_now = datetime.now(t_expire.tzinfo)
        if (t_expire - t_now).total_seconds() > delta:
            return False
    return True


#>>> Check internet connection <<<#
def check_internet_connection(url='http://www.google.com', timeout=5):
    try:
        http = urllib3.PoolManager()
        http.request('GET', url, timeout=urllib3.Timeout(connect=timeout, read=timeout))
        return True
    except urllib3.exceptions.HTTPError:
        return False


#>>> Renew AWS credentials (for Windows only) <<<#
def aws_creds_renew(seconds=0, delta=0, force=False, msg='AWS Credential Has Been Updated!'):
    global SESSION
    # Check if renewal is needed
    if not inWindows or (not force and not s3_is_expired(delta)):
        return SESSION

    # Retry Internet connection with max attempts
    for _ in range(30):  # Retry for ~5 minutes
        if check_internet_connection():
            break
        logger.info("Retrying Internet connection...")
        time.sleep(10)
    else:
        raise ConnectionError("Internet not available after retries")

    # Fetch credentials from environment
    usr = os.getenv('AWS_USR')
    pwd = os.getenv('AWS_PWD')
    host = os.getenv('AWS_HOST')
    token_url = os.getenv('AWS_TOKEN_URL')
    arn_url = os.getenv('AWS_ARN_URL')

    # Request temporary AWS credentials
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    resp = requests.post(token_url, headers={'Accept': '*/*'}, verify=False,
                         json={"username": usr, "password": pwd}).json()

    headers = {"Accept": "*/*", "Authorization": f"Bearer {resp['token']}"}
    creds = requests.get(arn_url, headers=headers, verify=False).json()['Credentials']

    # Update environment variables
    os.environ.update({
        'AWS_ACCESS_KEY_ID': creds['AccessKeyId'],
        'AWS_SECRET_ACCESS_KEY': creds['SecretAccessKey'],
        'AWS_SESSION_TOKEN': creds['SessionToken'],
        'HTTPS_PROXY': f'http://{usr}:{pwd}@{host}:8080'
    })
    msg and logger.debug(msg)

    # Create new boto3 session
    SESSION = boto3.Session(
        aws_access_key_id=creds['AccessKeyId'],
        aws_secret_access_key=creds['SecretAccessKey'],
        aws_session_token=creds['SessionToken'],
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )

    # Parse expiration time
    from dateutil import parser
    expire_time = parser.parse(creds['Expiration']).astimezone()
    setattr(SESSION, 'expire_time', expire_time)

    # Refresh S3 connection
    s3fs.S3FileSystem().connect(refresh=True)

    # Schedule renewal if seconds > 0
    if seconds > 0:
        threading.Timer(seconds, aws_creds_renew, args=(seconds,)).start()


#>>> AWS S3 utility manager for file operations <<<#
class S3Manager:

    #>>> Initialize S3 Manager: s3_bucket='s3://bucket', run_name='demo' <<<#
    def __init__(self, s3_bucket: str, run_name: str):
        self.s3_bucket = s3_bucket.rstrip('/')
        self.run_name = run_name
        self.base_path = f"{self.s3_bucket}/{self.run_name}"

        # Ensure credentials are fresh
        if inWindows:
            aws_creds_renew(delta=300)

    #>>> Ensure AWS credentials are up to date <<<#
    def _ensure_credentials(self):
        if inWindows:
            aws_creds_renew(delta=300)

    #>>> Get full S3 path for a file: step='meta_check', filename='results.pq' <<<#
    def get_s3_path(self, step: str, filename: str) -> str:
        return f"{self.base_path}/{step}/{filename}"

    #>>> Upload DataFrame as parquet to S3 <<<#
    def upload_parquet(self, df: pd.DataFrame, step: str, filename: str) -> str:
        self._ensure_credentials()

        if not filename.endswith('.pq'):
            filename = f"{filename}.pq"

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Uploading parquet to {s3_path}")

        aws.s3.to_parquet(
            df=df,
            path=s3_path,
            boto3_session=SESSION,
            compression='snappy'
        )

        logger.info(f"✓ Uploaded {filename} to {s3_path}")
        return s3_path

    #>>> Download parquet file from S3 as DataFrame <<<#
    def download_parquet(self, step: str, filename: str) -> pd.DataFrame:
        self._ensure_credentials()

        if not filename.endswith('.pq'):
            filename = f"{filename}.pq"

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Downloading parquet from {s3_path}")

        df = aws.s3.read_parquet(
            path=s3_path,
            boto3_session=SESSION
        )

        logger.info(f"✓ Downloaded {filename} ({len(df)} rows)")
        return df

    #>>> Upload JSON data to S3 <<<#
    def upload_json(self, data: dict, step: str, filename: str) -> str:
        self._ensure_credentials()

        if not filename.endswith('.json'):
            filename = f"{filename}.json"

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Uploading JSON to {s3_path}")

        # awswrangler expects DataFrame for to_json, so we convert dict
        import json
        json_str = json.dumps(data, indent=2)
        aws.s3.put_object(
            body=json_str.encode('utf-8'),
            bucket=self.s3_bucket.replace('s3://', ''),
            key=f"{self.run_name}/{step}/{filename}",
            boto3_session=SESSION
        )

        logger.info(f"✓ Uploaded {filename} to {s3_path}")
        return s3_path

    #>>> Download JSON file from S3 <<<#
    def download_json(self, step: str, filename: str) -> dict:
        self._ensure_credentials()

        if not filename.endswith('.json'):
            filename = f"{filename}.json"

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Downloading JSON from {s3_path}")

        import json
        obj = aws.s3.get_object(
            bucket=self.s3_bucket.replace('s3://', ''),
            key=f"{self.run_name}/{step}/{filename}",
            boto3_session=SESSION
        )
        data = json.loads(obj['Body'].read())

        logger.info(f"✓ Downloaded {filename}")
        return data

    #>>> Upload any file to S3 <<<#
    def upload_file(self, local_path: str, step: str, filename: str) -> str:
        self._ensure_credentials()

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Uploading {local_path} to {s3_path}")

        aws.s3.upload(
            local_file=local_path,
            path=s3_path,
            boto3_session=SESSION
        )

        logger.info(f"✓ Uploaded {filename} to {s3_path}")
        return s3_path

    #>>> Download any file from S3 <<<#
    def download_file(self, step: str, filename: str, local_path: str) -> str:
        self._ensure_credentials()

        s3_path = self.get_s3_path(step, filename)
        logger.info(f"Downloading {s3_path} to {local_path}")

        aws.s3.download(
            path=s3_path,
            local_file=local_path,
            boto3_session=SESSION
        )

        logger.info(f"✓ Downloaded to {local_path}")
        return local_path

    #>>> List files in S3 path <<<#
    def list_files(self, step: str, prefix: str = "") -> List[str]:
        self._ensure_credentials()

        s3_path = f"{self.base_path}/{step}/{prefix}"
        logger.info(f"Listing files in {s3_path}")

        files = aws.s3.list_objects(
            path=s3_path,
            boto3_session=SESSION
        )

        logger.info(f"✓ Found {len(files)} files")
        return files

    #>>> Upload multiple files from folder to S3 <<<#
    def upload_multiple(self, local_folder: str, step: str, pattern: str = '*.*') -> List[str]:
        folder = Path(local_folder)
        uploaded_paths = []

        for file in folder.glob(pattern):
            if file.is_file():
                s3_path = self.upload_file(str(file), step, file.name)
                uploaded_paths.append(s3_path)

        logger.info(f"✓ Uploaded {len(uploaded_paths)} files to S3")
        return uploaded_paths


#>>> Create S3Manager from environment variables <<<#
def create_s3_manager(run_name: Optional[str] = None) -> S3Manager:
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv('./src/input_pcds')

    s3_bucket = os.getenv('S3_BUCKET')
    if not s3_bucket:
        raise ValueError("S3_BUCKET not found in environment variables")

    if run_name is None:
        run_name = os.getenv('RUN_NAME', 'demo')

    return S3Manager(s3_bucket, run_name)


if __name__ == "__main__":
    # Test S3Manager
    import pandas as pd

    # Create test data
    test_df = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': ['a', 'b', 'c']
    })

    # Initialize manager
    s3 = create_s3_manager()

    # Test upload
    print("Testing parquet upload...")
    s3.upload_parquet(test_df, 'test', 'test_data')

    # Test download
    print("Testing parquet download...")
    df_downloaded = s3.download_parquet('test', 'test_data')
    print(df_downloaded)

    # Test JSON
    print("Testing JSON upload/download...")
    test_json = {'key': 'value', 'number': 42}
    s3.upload_json(test_json, 'test', 'test_meta')
    json_downloaded = s3.download_json('test', 'test_meta')
    print(json_downloaded)
