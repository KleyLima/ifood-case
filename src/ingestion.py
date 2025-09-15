import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import product
import boto3
from botocore.config import Config
import io
from os import getenv

TAXI_TYPES = [
    "fhvhv", # High Volume For-Hire Vehicle Trip Records
    "yellow",
    "green",
    "fhv" # For-Hire Vehicle Trip Records 
]

def check_and_create_bucket(bucket_name, region='us-east1'):
    print(f"Checking bucket {bucket_name}")
    session = boto3.Session(aws_access_key_id=getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY"),
                            region_name='us-east-1')

    s3 = session.client('s3', region_name='us-east-1')    
    response = s3.list_buckets()
    existing_buckets = [bucket['Name'] for bucket in response['Buckets']]
    if bucket_name in existing_buckets:
        print(f"Bucket '{bucket_name}' already exists")
    else:        
        print(f"Creating bucket {bucket_name}")
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully in region '{region}'")

def get_file_folder(filename: str):
    return filename.split("_")[0]

def download_file_stream_and_upload(url, filename=None, bucket_name="taxis-raw-data", chunk_size=15 * 1024 * 1024):
    session = boto3.Session(aws_access_key_id=getenv("AWS_ACCESS_KEY_ID"),
                            aws_secret_access_key=getenv("AWS_SECRET_ACCESS_KEY"),
                            region_name='us-east-1')
    config = Config(
        connect_timeout=5,
        read_timeout=60,
        max_pool_connections=500,
        retries={
            'max_attempts': 3,
            'mode': 'standard'
        }
    )
    s3 = session.client('s3', region_name='us-east-1', config=config)
    
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        if filename is None:
            content_disposition = response.headers.get('content-disposition')
            if content_disposition and 'filename=' in content_disposition:
                filename = content_disposition.split('filename=')[1].strip('"\'')
            else:
                filename = url.split('/')[-1] or 'downloaded_file'
                
        s3_file_key = f"{get_file_folder(filename)}/{filename}"
        print(f"Starting upload to: {s3_file_key}")
        
        multipart_upload = s3.create_multipart_upload(
            Bucket=bucket_name,
            Key=s3_file_key
        )
        upload_id = multipart_upload['UploadId']
        
        total_size = int(response.headers.get('content-length', 0))
        print(f"Downloading {filename}...")
        if total_size:
            print(f"File size: {total_size / (1024*1024):.2f} MB")
        
        parts = []
        part_number = 1
        downloaded = 0
        
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                try:
                    part_response = s3.upload_part(
                        Bucket=bucket_name,
                        Key=s3_file_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=io.BytesIO(chunk)
                    )
                    
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part_response['ETag']
                    })
                    
                    downloaded += len(chunk)
                    part_number += 1
                    
                    if total_size:
                        percent = (downloaded / total_size) * 100
                        print(f"\rProgress: {percent:.1f}% ({downloaded / (1024*1024):.2f}/{total_size / (1024*1024):.2f} MB)", end='')
                    else:
                        print(f"\rUploaded part {part_number - 1}, total size: {downloaded / (1024*1024):.2f} MB", end='')
                        
                except Exception as e:
                    print(f"\nError uploading part {part_number}: {e}")
                    s3.abort_multipart_upload(
                        Bucket=bucket_name,
                        Key=s3_file_key,
                        UploadId=upload_id
                    )
                    raise
        
        complete_response = s3.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_file_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"\nSuccessfully uploaded to s3://{bucket_name}/{s3_file_key}")
        return complete_response
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def get_all_date_combinations(start_date, end_date):
    date_format = "%Y-%m"
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    
    combinations = []
    current_year = start_date.year
    current_month = start_date.month
    
    end_year = end_date.year
    end_month = end_date.month
    
    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        combinations.append(f"{current_year}-{str(current_month).zfill(2)}")
        
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    
    return combinations

def all_download_combinations(start_date, end_date):
    """Create the base for all files available in TLC Trip Record Data (www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
    given two dates

    Args:
        start_date (str): Date in format "Year-Month"
        end_date (str): Date in format "Year-Month"
    
    Returns: A list with all possible file names.
    """
    date_combinations = get_all_date_combinations(start_date, end_date)
    combinations = [f"{taxi}_tripdata_{data}.parquet" for taxi, data in product(TAXI_TYPES, date_combinations)]    

    print(f"Got {len(combinations)} possible files to download")
    return combinations

def download_files(start_date, end_date):
    file_names = all_download_combinations(start_date, end_date)
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    check_and_create_bucket("taxis-raw-data")
    
    urls_to_download = [base_url + file_name for file_name in file_names]
    with ThreadPoolExecutor() as executor:
        executor.map(download_file_stream_and_upload, urls_to_download)

if __name__ == "__main__":    
    download_files("2023-01", "2023-05")
