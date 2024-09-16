import os
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import psycopg2
from psycopg2.extras import execute_values
import time

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = '1s1wErU_ty3XTGRqcN2J3J5vEA7BlMruhHzgFP8xgnnM'
SAMPLE_RANGE_NAME = 'Employees!A1:F1000'

DB_NAME = "superjoin"
DB_USER = "postgres"
DB_PASSWORD = "roosh123"
DB_HOST = "localhost"
DB_PORT = "5432"

last_sync_time = None

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
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                email VARCHAR(100),
                department VARCHAR(100),
                salary NUMERIC(16, 2)
            )
            """)
            conn.commit()

def fetch_sheet_data(service, spreadsheet_id, range_name):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    
    if not values:
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])
    
    df = pd.DataFrame(values[1:], columns=values[0]) 
    
    # Convert id to integer and salary to float
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce')
    
    # Remove rows with any empty cells
    df = df.dropna()
    
    return df

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
    values.insert(0, df.columns.tolist())  # Add column headers
    
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='RAW', body=body).execute()

def fetch_db_data():
    with get_db_connection() as conn:
        query = "SELECT id, first_name, last_name, email, department, salary FROM employees ORDER BY id"
        return pd.read_sql_query(query, conn)

def update_db_data(df):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE employees")
            execute_values(cur, """
                INSERT INTO employees (id, first_name, last_name, email, department, salary)
                VALUES %s
            """, [tuple(x) for x in df.values])
        conn.commit()

def sync_data():
    global last_sync_time
    
    sheets_service = get_google_sheets_service()
    
    try:
        create_table_if_not_exists()
        
        sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        db_df = fetch_db_data()
        
        # Merge data, preferring sheet data over db data
        merged_df = pd.concat([db_df, sheet_df]).drop_duplicates(subset='id', keep='last')
        merged_df = merged_df.sort_values('id').reset_index(drop=True)
        
        # Ensure data types
        merged_df['id'] = pd.to_numeric(merged_df['id'], errors='coerce').astype(int)
        merged_df['salary'] = pd.to_numeric(merged_df['salary'], errors='coerce').astype(float)
        
        # Remove rows with any empty cells
        merged_df = merged_df.dropna()
        
        # ensuring salary is in valid range
        merged_df['salary'] = merged_df['salary'].clip(lower=0, upper=99999999999999.99)
        
        # Update database
        update_db_data(merged_df)
        
        # Update sheet
        update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, merged_df)
        
        last_sync_time = time.time()
        
        return merged_df
    except Exception as e:
        print(f"Error during sync: {e}")
        # If sync fails, return the latest data from the database
        return fetch_db_data()

def load_data():
    try:
        return sync_data()
    except Exception as e:
        print(f"Error loading data: {e}")
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])

def save_data(df):
    sheets_service = get_google_sheets_service()
    
    try:
        update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, df)
        update_db_data(df)
    except Exception as e:
        print(f"Error saving data: {e}")
        raise

def delete_record(record_id):
    sheets_service = get_google_sheets_service()
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM employees WHERE id = %s", (record_id,))
            conn.commit()
        
        sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
        sheet_df = sheet_df[sheet_df['id'] != int(record_id)]
        update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, sheet_df)
    except Exception as e:
        print(f"Error deleting record: {e}")
        raise

def get_last_update_times():
    return last_sync_time, last_sync_time

def poll_for_changes():
    while True:
        try:
            sync_data()
        except Exception as e:
            print(f"Error during synchronization: {e}")
        time.sleep(10)  # Poll every 10 seconds