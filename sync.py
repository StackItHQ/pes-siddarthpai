import os
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
import psycopg2
import time
from pprint import pprint
from googleapiclient.errors import HttpError

# sheets creds
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = '1s1wErU_ty3XTGRqcN2J3J5vEA7BlMruhHzgFP8xgnnM'
SAMPLE_RANGE_NAME = 'Employees!A1:F1000'  

# psql creds
DB_NAME = "superjoin"
DB_USER = "postgres"
DB_PASSWORD = "roosh123"
DB_HOST = "localhost"
DB_PORT = "5432"

def get_google_sheets_service():
    creds = None
    # starting a local flow to get tokens fixed my scope issue as well helps making the system more secure ensuring not everyone has access to the CRUD of the database
    flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
    creds = flow.run_local_server(port=0)
    
    # Save the credentials for the next run
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

def log_data(title, data):
    print(f"\n{title}")
    print("-" * 50)
    pprint(data)
    print("-" * 50)

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
    return result.get('values', [])

def update_sheet_data(service, spreadsheet_id, range_name, values):
    body = {'values': values}
    try:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption='USER_ENTERED', body=body).execute()
        print("Successfully updated Google Sheets")
    except HttpError as error:
        print(f"An error occurred while updating Google Sheets: {error}")
        if error.resp.status == 401 or error.resp.status == 403: #refresh token in case of error 
            print("Attempting to refresh authentication...")
            service = get_google_sheets_service()
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id, range=range_name,
                    valueInputOption='USER_ENTERED', body=body).execute()
                print("Successfully updated Google Sheets after refreshing authentication")
            except HttpError as refresh_error:
                print(f"Failed to update even after refreshing authentication: {refresh_error}")

def fetch_db_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM employees ORDER BY id")
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def update_db_data(conn, data):
    with conn.cursor() as cur:
        for row in data[1:]:  # header skip
            row = row + [None] * (6 - len(row))
            
            # for all the below lines, i try converting the data from google sheets into 
            # the correct type and in case of mismatch, use None 
            try:
                id_val = int(row[0])
            except ValueError:
                print(f"Warning: Invalid ID '{row[0]}'. Skipping this row.")
                continue
            
            row = [None if val == '' else val for val in row]
            
            try:
                salary = float(row[5]) if row[5] is not None else None
            except ValueError:
                print(f"Warning: Invalid salary '{row[5]}' for ID {id_val}. Setting to None.")
                salary = None
            
            try:
                cur.execute("""
                INSERT INTO employees (id, first_name, last_name, email, department, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    department = EXCLUDED.department,
                    salary = EXCLUDED.salary
                """, (id_val, row[1], row[2], row[3], row[4], salary))
            except Exception as e:
                print(f"Error inserting/updating row with ID {id_val}: {e}")
                conn.rollback()  # Rollback the transaction for this row
            else:
                conn.commit()  # Commit the transaction if successful

def sync_data():
    sheets_service = get_google_sheets_service()
    db_conn = get_db_connection()

    create_table_if_not_exists(db_conn)

    while True:
        print("\n" + "="*50)
        print("Starting sync cycle")
        print("="*50)

        try:
            sheet_data = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)
            db_data = fetch_db_data(db_conn)

            log_data("Current Google Sheet Data:", sheet_data)
            log_data("Current Database Data:", db_data)

            db_data_formatted = [[str(value) for value in row.values()] for row in db_data]
            db_data_formatted.insert(0, list(db_data[0].keys()) if db_data else [])

            if sheet_data != db_data_formatted:
                print("\nDifferences detected. Updating...")
                
                update_db_data(db_conn, sheet_data)
                print("Updated database from Google Sheets")

                updated_db_data = fetch_db_data(db_conn)
                
                updated_db_formatted = [[str(value) for value in row.values()] for row in updated_db_data]
                updated_db_formatted.insert(0, list(updated_db_data[0].keys()) if updated_db_data else [])

                update_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME, updated_db_formatted)

                updated_sheet_data = fetch_sheet_data(sheets_service, SAMPLE_SPREADSHEET_ID, SAMPLE_RANGE_NAME)

                log_data("Updated Google Sheet Data:", updated_sheet_data)
                log_data("Updated Database Data:", updated_db_data)
            else:
                print("\nNo differences detected. No updates needed.")

        except Exception as e:
            print(f"An error occurred during the sync cycle: {e}")

        print("\nSync cycle completed. Waiting 60 seconds before next cycle...")
        time.sleep(60) 

if __name__ == "__main__":
    sync_data()