import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from itertools import product

TAXI_TYPES = [
    "fhvhv", # High Volume For-Hire Vehicle Trip Records
    "yellow",
    "green",
    "fhv" # For-Hire Vehicle Trip Records 
]

def download_file_stream(url, filename=None, chunk_size=15 * 1024 * 1024):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    if filename is None:
        content_disposition = response.headers.get('content-disposition')
        if content_disposition and 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"\'')
        else:
            filename = url.split('/')[-1] or 'downloaded_file'
    
    total_size = int(response.headers.get('content-length', 0))
    
    print(f"Downloading {filename}...\n")
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

    return combinations

def download_files(start_date, end_date):
    file_names = all_download_combinations(start_date, end_date)
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    urls_to_download = [base_url + file_name for file_name in file_names]
    with ThreadPoolExecutor() as executor:
        executor.map(download_file_stream, urls_to_download)

if __name__ == "__main__":    
    # print(get_all_date_combinations("2023-01", "2025-05"))
    # print(all_download_combinations("2023-01", "2023-05"))
    download_files("2023-01", "2023-05")