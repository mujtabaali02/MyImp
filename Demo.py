import requests
import datetime
import os
import pandas as pd
# from google.oauth2 import service_account
# from googleapiclient.discovery import build

def download_csv_files(base_url, download_folder):
    num_files_downloaded = 0
    downloaded_files = []

    current_date = datetime.datetime.now()
    
    if current_date.time() < datetime.time(12, 30):
        current_date -= datetime.timedelta(days=1)
        hour = '23.59'
    elif datetime.time(12, 30) <= current_date.time() < datetime.time(14, 30):
        hour = '12.00'
    elif datetime.time(14, 30) <= current_date.time() < datetime.time(16, 30):
        hour = '14.00'    
    elif datetime.time(16, 30) <= current_date.time() < datetime.time(19, 0):
        hour = '16.00'
    elif datetime.time(19, 0) <= current_date.time() < datetime.time(21, 30):
        hour = '18.30'
    elif datetime.time(20, 30) <= current_date.time() < datetime.time(22, 30):
        hour = '20.00'
    else:
        hour = '22.00'

    csv_filename_template = f"EkartReport-LAST_MILE-FWD-{current_date.strftime('%Y.%m.%d')}-{hour}-{{file_number}}.csv"
    
    file_number = 1
    while True:
        csv_filename = csv_filename_template.format(file_number=file_number)
        csv_filepath = os.path.join(download_folder, csv_filename)
        
        if os.path.exists(csv_filepath):
            print(f"File already exists: {csv_filename}")
            downloaded_files.append(csv_filepath)
            num_files_downloaded += 1
        else:
            csv_url = base_url + csv_filename
            response = requests.get(csv_url)
            if response.status_code == 200:
                with open(csv_filepath, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded CSV file: {csv_filename}")
                downloaded_files.append(csv_filepath)
                num_files_downloaded += 1
            else:
                break
        
        file_number += 1

    return num_files_downloaded, downloaded_files

def merge_csv_files(downloaded_files, download_folder):
    if len(downloaded_files) < 1:
        print("No CSV files downloaded. Exiting merge operation.")
        return None, None
    
    dfs = []
    for csv_file in downloaded_files:
        df = pd.read_csv(csv_file, low_memory=False)
        dfs.append(df)
    
    merged_df = pd.concat(dfs, ignore_index=True)
    
    merged_df = merged_df[merged_df['zone'] == 'North']
    
    merged_df = merged_df[~merged_df['hub_name'].str.contains('mynt', case=False)]
    
    merged_df = merged_df[~merged_df['fake_detection_reason'].str.contains('SPOOF', case=False)]
    
    merged_df = merged_df.drop_duplicates(subset='vendor_tracking_id', keep='first')
    
    return merged_df

if __name__ == "__main__":
    base_url = "http://10.24.44.15/fake-detection-reports/LAST_MILE/FORWARD/"
    download_folder = os.path.expanduser('~') + '/Downloads/'
    credentials_file = 'cx-dashboard-426911-991c82f4850e.json'
    sheet_id = '1IKE_AlOfUqIF7-9BWsESn611Wg-WmbPBTrLhVC5kf3w'
    range_name = 'Raw Data!A2'
    
    num_files_downloaded, downloaded_files = download_csv_files(base_url, download_folder)
    
    print(f"Total number of files downloaded: {num_files_downloaded}")
    df = merge_csv_files(downloaded_files, download_folder)
    # print(df)