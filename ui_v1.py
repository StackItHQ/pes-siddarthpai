import os
import streamlit as st
import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import psycopg2
from psycopg2.extras import execute_values
from googleapiclient.errors import HttpError
import time
import threading
from decimal import Decimal

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


@st.cache_resource
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

@st.cache_resource
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
    # for dec -> float for JSON serialization
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
    
    # Handle empty dataframes
    if db_df.empty and sheet_df.empty:
        return pd.DataFrame(columns=['id', 'first_name', 'last_name', 'email', 'department', 'salary'])
    
    # Convert 'id' column to string for consistent comparison
    db_df['id'] = db_df['id'].astype(str)
    sheet_df['id'] = sheet_df['id'].astype(str)
    
    # Identify added, updated, and deleted records
    db_ids = set(db_df['id'])
    sheet_ids = set(sheet_df['id'])
    
    # Handle additions from both sources
    db_added = db_df[db_df['id'].isin(db_ids - sheet_ids)]
    sheet_added = sheet_df[sheet_df['id'].isin(sheet_ids - db_ids)]
    
    # Handle common records (potential updates)
    common_ids = db_ids.intersection(sheet_ids)
    updated_records = []
    for id in common_ids:
        db_row = db_df[db_df['id'] == id].iloc[0]
        sheet_row = sheet_df[sheet_df['id'] == id].iloc[0]
        if not db_row.equals(sheet_row):
            # use most recent update in case of difference
            if last_db_update and last_sheet_update:
                updated_records.append(db_row if last_db_update > last_sheet_update else sheet_row)
            else:
                # preference to sheet if no timestamps 
                updated_records.append(sheet_row)
    
    # Combine all records
    merged_df = pd.concat([
        db_df[db_df['id'].isin(common_ids)],  # existing values
        db_added,  # new from psql db
        sheet_added,  # new from sheet
        pd.DataFrame(updated_records)  # updated
    ]).drop_duplicates(subset='id', keep='last')
    
    # Sort the dataframe by id
    merged_df = merged_df.sort_values('id').reset_index(drop=True)
    
    # update both db and sheets with the merged data
    update_db_data(db_conn, merged_df)
    update_sheet_data(get_google_sheets_service(), SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, merged_df)
    
    last_db_update = time.time()
    last_sheet_update = time.time()
    
    return merged_df

@st.cache_data(ttl=1)  # Cache for 1 second to ensure fresh data on each load
def load_data():
    # Clear the cache to ensure fresh data
    st.cache_data.clear()
    
    db_conn = get_db_connection()
    sheets_service = get_google_sheets_service()
    create_table_if_not_exists(db_conn)
    
    sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
    df = sync_data(sheet_df, db_conn)
    
    return df

def save_data(sheets_service, db_conn, df):
    global last_sheet_update, last_db_update
    
    # Update Google Sheets
    update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, df)
    last_sheet_update = time.time()
    
    # Update Database
    update_db_data(db_conn, df)
    last_db_update = time.time()

def delete_record(sheets_service, db_conn, record_id):
    global last_sheet_update, last_db_update
    
    # Delete from database
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM employees WHERE id = %s", (record_id,))
    db_conn.commit()
    last_db_update = time.time()

    # Delete from Google Sheets
    sheet_df = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
    sheet_df = sheet_df[sheet_df['id'] != str(record_id)]
    update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, sheet_df)
    last_sheet_update = time.time()

def auto_refresh():
    while True:
        time.sleep(20)
        st.session_state['refresh'] = True

def main():
    st.title("Employee Management System")


    if 'refresh' not in st.session_state:
        st.session_state['refresh'] = False

    if st.session_state['refresh']:
        st.session_state['refresh'] = False
        st.rerun()

    if 'auto_refresh_started' not in st.session_state:
        st.session_state['auto_refresh_started'] = True
        threading.Thread(target=auto_refresh, daemon=True).start()

    db_conn = get_db_connection()
    sheets_service = get_google_sheets_service()
    df = load_data()

    st.sidebar.header("Actions")
    action = st.sidebar.radio("Choose an action", ["View Data", "Add Employee", "Edit Employee", "Delete Employee"])

    if action == "View Data":
        st.header("Employee Data")
        st.dataframe(df)

    elif action == "Add Employee":
        st.header("Add New Employee")
        new_id = st.number_input("ID", min_value=1, step=1, value=df['id'].astype(int).max() + 1 if not df.empty else 1)
        first_name = st.text_input("First Name")
        last_name = st.text_input("Last Name")
        email = st.text_input("Email")
        department = st.text_input("Department")
        salary = st.number_input("Salary", min_value=0.0, step=100.0)

        if st.button("Add Employee"):
            new_row = pd.DataFrame({
                "id": [new_id],
                "first_name": [first_name],
                "last_name": [last_name],
                "email": [email],
                "department": [department],
                "salary": [salary]
            })
            updated_df = pd.concat([df, new_row], ignore_index=True)
            save_data(sheets_service, db_conn, updated_df)
            st.success("Employee added successfully!")
            st.rerun()

    elif action == "Edit Employee":
        st.header("Edit Employee")
        employee_id = st.selectbox("Select Employee ID", df['id'].tolist())
        employee = df[df['id'] == employee_id].iloc[0]

        first_name = st.text_input("First Name", employee['first_name'])
        last_name = st.text_input("Last Name", employee['last_name'])
        email = st.text_input("Email", employee['email'])
        department = st.text_input("Department", employee['department'])
        salary = st.number_input("Salary", min_value=0.0, step=100.0, value=float(employee['salary']))

        if st.button("Update Employee"):
            df.loc[df['id'] == employee_id, ['first_name', 'last_name', 'email', 'department', 'salary']] = [first_name, last_name, email, department, salary]
            save_data(sheets_service, db_conn, df)
            st.success("Employee updated successfully!")
            st.rerun()

    elif action == "Delete Employee":
        st.header("Delete Employee")
        employee_id = st.selectbox("Select Employee ID to Delete", df['id'].tolist())

        if st.button("Delete Employee"):
            delete_record(sheets_service, db_conn, employee_id)
            st.success("Employee deleted successfully!")
            st.rerun()

    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    # Display last update times
    st.sidebar.write(f"Last Sheet Update: {pd.to_datetime(last_sheet_update, unit='s') if last_sheet_update else 'Never'}")
    st.sidebar.write(f"Last DB Update: {pd.to_datetime(last_db_update, unit='s') if last_db_update else 'Never'}")

if __name__ == "__main__":
    main()