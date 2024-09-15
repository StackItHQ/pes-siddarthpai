import os
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import psycopg2
from psycopg2.extras import execute_values
from decimal import Decimal
import time

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = '1s1wErU_ty3XTGRqcN2J3J5vEA7BlMruhHzgFP8xgnnM'
SAMPLE_RANGE_NAME = 'Employees!A1:F1000'

DB_NAME = "superjoin"
DB_USER = "postgres"
DB_PASSWORD = "roosh123"
DB_HOST = "localhost"
DB_PORT = "5432"

last_sheet_update = None
last_db_update = None

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
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    return conn

def create_table_if_not_exists(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(100),
            department VARCHAR(100),
            salary NUMERIC(10, 2)
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
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce')
    return df

def update_sheet_data(service, spreadsheet_id, range_name, df):
    df = df.astype(object).where(pd.notnull(df), None)
    df = df.applymap(lambda x: float(x) if isinstance(x, Decimal) else x)
    
    values = [df.columns.tolist()] + df.values.tolist()
    body = {'values': values}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption='USER_ENTERED', body=body).execute()

def fetch_db_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, first_name, last_name, email, department, salary FROM employees ORDER BY id")
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)

def update_db_data(conn, df):
    df = df.astype(object).where(pd.notnull(df), None)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE employees")
        execute_values(cur, """
            INSERT INTO employees (id, first_name, last_name, email, department, salary)
            VALUES %s
        """, [tuple(x) for x in df.values])
    conn.commit()

def sync_data(sheet_df, db_conn):
    global last_sheet_update, last_db_update
    
    db_df = fetch_db_data(db_conn)
    
    if db_df.empty and sheet_df.empty:
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])
    
    db_df['id'] = db_df['id'].astype(str)
    sheet_df['id'] = sheet_df['id'].astype(str)
    
    db_ids = set(db_df['id'])
    sheet_ids = set(sheet_df['id'])
    
    db_added = db_df[db_df['id'].isin(db_ids - sheet_ids)]
    sheet_added = sheet_df[sheet_df['id'].isin(sheet_ids - db_ids)]
    
    common_ids = db_ids.intersection(sheet_ids)
    updated_records = []
    for id in common_ids:
        db_row = db_df[db_df['id'] == id].iloc[0]
        sheet_row = sheet_df[sheet_df['id'] == id].iloc[0]
        if not db_row.equals(sheet_row):
            if last_db_update and last_sheet_update:
                updated_records.append(db_row if last_db_update > last_sheet_update else sheet_row)
            else:
                updated_records.append(sheet_row)
    
    merged_df = pd.concat([
        db_df[db_df['id'].isin(common_ids)],
        db_added,
        sheet_added,
        pd.DataFrame(updated_records)
    ]).drop_duplicates(subset='id', keep='last')
    
    merged_df = merged_df.sort_values('id').reset_index(drop=True)
    
    update_db_data(db_conn, merged_df)
    update_sheet_data(get_google_sheets_service(), SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, merged_df)
    
    last_db_update = time.time()
    last_sheet_update = time.time()
    
    return merged_df

def load_data():
    db_conn = get_db_connection()
    sheets_service = get_google_sheets_service()
    create_table_if_not_exists(db_conn)
    
    sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
    df = sync_data(sheet_df, db_conn)
    
    return df

def save_data(df):
    global last_sheet_update, last_db_update
    
    sheets_service = get_google_sheets_service()
    db_conn = get_db_connection()
    
    update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, df)
    last_sheet_update = time.time()
    
    update_db_data(db_conn, df)
    last_db_update = time.time()

def delete_record(record_id):
    global last_sheet_update, last_db_update
    
    sheets_service = get_google_sheets_service()
    db_conn = get_db_connection()
    
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM employees WHERE id = %s", (record_id,))
    db_conn.commit()
    last_db_update = time.time()

    sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
    sheet_df = sheet_df[sheet_df['id'] != str(record_id)]
    update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, sheet_df)
    last_sheet_update = time.time()

def get_last_update_times():
    return last_sheet_update, last_db_update