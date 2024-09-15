import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:password@localhost/dbname')
GOOGLE_SHEETS_CREDENTIALS_FILE = 'credentials.json' #got from the gcp page
GOOGLE_SHEETS_TOKEN_FILE = 'token.json'
GOOGLE_SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = '1s1wErU_ty3XTGRqcN2J3J5vEA7BlMruhHzgFP8xgnnM'
SAMPLE_RANGE_NAME = 'Sheet1!A1:B'