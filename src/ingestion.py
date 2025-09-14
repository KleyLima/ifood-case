import requests
import os
from concurrent.futures import ThreadPoolExecutor

base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_2023-01.parquet"

TAXI_TYPES = [
    "fhvhv", # High Volume For-Hire Vehicle Trip Records
    "yellow",
    "green",
    "fhv" # For-Hire Vehicle Trip Records 
]

def download_file_stream(url, filename=None, chunk_size=30 * 1024):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    if filename is None:
        content_disposition = response.headers.get('content-disposition')
        if content_disposition and 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"\'')
        else:
            filename = url.split('/')[-1] or 'downloaded_file'
    
    total_size = int(response.headers.get('content-length', 0))
    
    print(f"Downloading {filename}...")
    if total_size:
        print(f"File size: {total_size / (1024*1024):.2f} MB")
    
    downloaded = 0
    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                file.write(chunk)
                downloaded += len(chunk)
                
                if total_size:
                    percent = (downloaded / total_size) * 100
                    print(f"\r{filename} Progress: {percent:.1f}% ({downloaded  / (1024*1024):.2f}/{total_size  / (1024*1024):.2f} MB)", end='')
    
    print(f"\nDownload completed: {filename}")
    return filename

if __name__ == "__main__":
    file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    
    with ThreadPoolExecutor() as executor:
        executor.map(download_file_stream, [base_url.format(taxi_type=t_type) for t_type in TAXI_TYPES])
    