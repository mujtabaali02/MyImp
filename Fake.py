import requests
import datetime
import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

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
    
    new_column_name = 'fake_detection_reason1'
    merged_df.insert(merged_df.columns.get_loc('fake_detection_reason') + 1, new_column_name, '')
    
    merged_df.loc[(merged_df['fake_detection_status'] == 'GENUINE'), new_column_name] = 'Genuine Attempt'
    merged_df.loc[(merged_df['fake_detection_status'] == 'FAKE') & (merged_df['fake_detection_reason'] == 'GEO_FAKE'), new_column_name] = 'GEO_FAKE'
    merged_df.loc[(merged_df['fake_detection_status'] == 'FAKE') & (merged_df['fake_detection_reason'] == 'NO_CALL_FAKE'), new_column_name] = 'NO_CALL_FAKE'
    merged_df.loc[(merged_df['fake_detection_status'] == 'FAKE') & (merged_df['undel_unpick_status'] == 'DELIVERED'), new_column_name] = 'DELIVERED_GEO_FAKE'
    merged_df.loc[(merged_df['fake_detection_status'] == 'FAKE') & (merged_df['fake_detection_reason'] == 'IVR_FAKE'), new_column_name] = 'IVR_Fake'
    merged_df.loc[(merged_df['fake_detection_status'] == 'FAKE') & (merged_df['fake_detection_reason'] == 'NO_CALL_FAKE_UDBad_Fake'), new_column_name] = 'NO_CALL_FAKE_UDBad_Fake'
    
    merged_df.loc[merged_df[new_column_name] == '', new_column_name] = 'INVALID_CALL_FAKE'
    
    escalation_matrix_file = "Escalation Matrix - DH.xlsx"
    escalation_matrix_df = pd.read_excel(os.path.join(download_folder, escalation_matrix_file))
    
    for i, column_name in enumerate(['L3', 'L2', 'L1']):
        merged_df['hub_name_lower'] = merged_df['hub_name'].str.lower()
        escalation_matrix_df['Hub Name_lower'] = escalation_matrix_df['Hub Name'].str.lower()
        
        merged_df = pd.merge(merged_df, escalation_matrix_df[['Hub Name_lower', column_name]], how='left', left_on='hub_name_lower', right_on='Hub Name_lower')
        
        merged_df.drop(columns=['hub_name_lower', 'Hub Name_lower'], inplace=True)
    
    def categorize_vendor_tracking_id(id):
        if 'FKH' in id:
            return 'Flipkart Health'
        elif 'FMR' in id:
            return 'Flipkart Mail Room'
        elif 'FMP' in id:
            return 'Flipkart'
        elif 'MYE' in id or 'MYS' in id or 'MYN' in id:
            return 'Myntra'
        else:
            return 'External'
    
    merged_df['Category'] = merged_df['vendor_tracking_id'].apply(categorize_vendor_tracking_id)
    
    max_rows_per_file = 700000
    total_rows = merged_df.shape[0]
    num_files = (total_rows // max_rows_per_file) + 1 if total_rows % max_rows_per_file != 0 else total_rows // max_rows_per_file
    
    summary_df = merged_df.groupby(['hub_name', 'L3', 'L2', 'L1', 'fake_detection_reason1']).size().unstack(fill_value=0)
    
    all_reasons = ['DELIVERED_GEO_FAKE', 'GEO_FAKE', 'Genuine Attempt', 'INVALID_CALL_FAKE', 'NO_CALL_FAKE', 'IVR_Fake', 'UDBad_Fake']
    
    summary_df = summary_df.reindex(columns=all_reasons, fill_value=0)
    
    summary_df = summary_df.loc[:, ~summary_df.columns.duplicated()]
    
    summary_filename = "EkartReports_Summary.csv"
    summary_filepath = os.path.join(download_folder, summary_filename)
    summary_df.to_csv(summary_filepath)
    print(f"Summary CSV file saved as: {summary_filepath}")

    merged_df = merged_df[merged_df['fake_detection_status'] != 'GENUINE']
    merged_df = merged_df[merged_df['L1'].notnull()]

    for i in range(num_files):
        start_row = i * max_rows_per_file
        end_row = min(start_row + max_rows_per_file, total_rows)
        split_df = merged_df.iloc[start_row:end_row]
        
        split_filename = f"Filtered_EkartReports_Part{i+1}.csv"
        split_filepath = os.path.join(download_folder, split_filename)
        split_df.to_csv(split_filepath, index=False)
        print(f"Filtered CSV file saved as: {split_filepath}")
    
    for file_path in downloaded_files:
        os.remove(file_path)
        print(f"Deleted filtered CSV file: {file_path}")
    
    return summary_filepath, summary_df

def upload_to_google_sheets(summary_df, sheet_id, range_name, credentials_file):
    credentials_path = os.path.join(os.path.expanduser("~"), "Downloads", credentials_file)
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    service = build('sheets', 'v4', credentials=credentials)
    
    sheet = service.spreadsheets()
    
    values = summary_df.reset_index().values.tolist()
    
    body = {
        'values': values
    }
    
    result = sheet.values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    
    print(f"{result.get('updatedCells')} cells updated in Google Sheet.")
    
    if summary_filepath:
        os.remove(summary_filepath)
        print(f"Deleted summary CSV file: {summary_filepath}")

if __name__ == "__main__":
    base_url = "http://10.24.44.15/fake-detection-reports/LAST_MILE/FORWARD/"
    download_folder = os.path.expanduser('~') + '/Downloads/'
    credentials_file = 'cx-dashboard-426911-991c82f4850e.json'
    sheet_id = '1IKE_AlOfUqIF7-9BWsESn611Wg-WmbPBTrLhVC5kf3w'
    range_name = 'Raw Data!A2'
    
    num_files_downloaded, downloaded_files = download_csv_files(base_url, download_folder)
    print(f"Total number of files downloaded: {num_files_downloaded}")
    
    summary_filepath, summary_df = merge_csv_files(downloaded_files, download_folder)
    
    if summary_df is not None:
        upload_to_google_sheets(summary_df, sheet_id, range_name, credentials_file)