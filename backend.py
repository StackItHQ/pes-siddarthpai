import os
import pandas as pd
import numpy as np
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import psycopg2
from psycopg2.extras import execute_values
import time
import threading

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = '1s1wErU_ty3XTGRqcN2J3J5vEA7BlMruhHzgFP8xgnnM'
SAMPLE_RANGE_NAME = 'Employees!A1:F1000'

DB_NAME = "superjoin"
DB_USER = "postgres"
DB_PASSWORD = "roosh123"
DB_HOST = "localhost"
DB_PORT = "5432"

last_sheet_sync_time = None
last_db_sync_time = None
data_changed = threading.Event()

def get_google_sheets_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('sheets', 'v4', credentials=creds)

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

def create_table_if_not_exists():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id INTEGER PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                department VARCHAR(100) NOT NULL,
                salary NUMERIC(16, 2) NOT NULL
            )
            """)
            conn.commit()

def validate_and_clean_data(df):
    # Ensure id is integer
    df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
    
    # Ensure salary is float
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce').astype(float)
    
    # Ensure other fields are strings and not empty
    string_columns = ['first_name', 'last_name', 'email', 'department']
    for col in string_columns:
        df[col] = df[col].astype(str).replace('', np.nan)
    
    # Drop rows with any null values
    df = df.dropna()
    
    # Clip salary to valid range
    df['salary'] = df['salary'].clip(lower=0, upper=99999999999999.99)
    
    return df

def fetch_sheet_data(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    if not values:
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])
    
    df = pd.DataFrame(values[1:], columns=values[0])
    return validate_and_clean_data(df)

def update_sheet_data(service, spreadsheet_id, range_name, df):
    df = df.astype(object).where(pd.notnull(df), None)
    
    def format_value(val):
        if pd.isna(val):
            return ""
        elif isinstance(val, (int, float)):
            return str(val)
        else:
            return val

    values = [[format_value(val) for val in row] for row in df.values.tolist()]
    values.insert(0, df.columns.tolist())
    
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='RAW', body=body).execute()

def fetch_db_data():
    with get_db_connection() as conn:
        query = "SELECT id, first_name, last_name, email, department, salary FROM employees ORDER BY id"
        df = pd.read_sql_query(query, conn)
        return validate_and_clean_data(df)

def update_db_data(df):
    df = validate_and_clean_data(df)
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare data for upsert
            data = [tuple(x) for x in df.to_numpy()]
            
            # Perform upsert operation
            execute_values(cur, """
                INSERT INTO employees (id, first_name, last_name, email, department, salary)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    department = EXCLUDED.department,
                    salary = EXCLUDED.salary
            """, data)
        conn.commit()

def sync_data():
    global last_sheet_sync_time, last_db_sync_time, data_changed
    
    sheets_service = get_google_sheets_service()
    
    try:
        create_table_if_not_exists()
        
        sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        db_df = fetch_db_data()
        
        # Identify deleted records
        sheet_ids = set(sheet_df['id'].astype(int))
        db_ids = set(db_df['id'].astype(int))
        deleted_ids = db_ids - sheet_ids
        
        # Remove deleted records from the database
        if deleted_ids:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM employees WHERE id IN %s", (tuple(map(int, deleted_ids)),))
                conn.commit()
            db_df = db_df[~db_df['id'].isin(deleted_ids)]
            data_changed.set()
        
        # Merge data, preferring sheet data for updates
        merged_df = pd.concat([db_df, sheet_df]).drop_duplicates(subset='id', keep='last')
        merged_df = merged_df.sort_values('id').reset_index(drop=True)
        
        # Validate and clean the merged data
        merged_df = validate_and_clean_data(merged_df)
        
        # Convert numpy types to native Python types
        merged_df = merged_df.astype({
            'id': int,
            'first_name': str,
            'last_name': str,
            'email': str,
            'department': str,
            'salary': float
        })
        
        # Update database if there are changes
        if not merged_df.equals(db_df):
            update_db_data(merged_df)
            last_db_sync_time = time.time()
            data_changed.set()
        
        # Update sheet if there are changes
        if not merged_df.equals(sheet_df):
            update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, merged_df)
            last_sheet_sync_time = time.time()
            data_changed.set()
        
        return merged_df
    except Exception as e:
        print(f"Error during sync: {e}")
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])
    

def load_data():
    return sync_data()

def save_data(df):
    try:
        df = validate_and_clean_data(df)
        update_db_data(df)
        sync_data()  # This will ensure the sheet is updated as well
        data_changed.set()
    except Exception as e:
        print(f"Error saving data: {e}")
        raise

def delete_record(record_id):
    try:
        # Ensure record_id is an integer
        record_id = int(record_id)
        
        # Delete from database
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM employees WHERE id = %s", (record_id,))
            conn.commit()
        
        # Delete from sheet
        sheets_service = get_google_sheets_service()
        sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        sheet_df = sheet_df[sheet_df['id'] != record_id]
        update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, sheet_df)
        
        data_changed.set()
    except ValueError:
        print(f"Error: Invalid record ID. Must be an integer.")
    except Exception as e:
        print(f"Error deleting record: {e}")
        raise

def get_last_update_times():
    return last_sheet_sync_time, last_db_sync_time

def poll_for_changes():
    while True:
        try:
            sync_data()
        except Exception as e:
            print(f"Error during synchronization: {e}")
        time.sleep(10)  # Poll every 10 seconds

def has_data_changed():
    return data_changed.is_set()

def reset_data_changed():
    data_changed.clear()