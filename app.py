from flask import Flask,jsonify
import config

app = Flask(__name__)

# importing configurations from the config file.
database_url = config.DATABASE_URL
spreadsheet_id = config.SAMPLE_SPREADSHEET_ID

@app.route('/')
def hello():
    return f"Hello, Google Sheets DB Sync! Using database: {database_url}" #testing Flask webserver => localhost:5000/home will show this!

@app.route('/config') #for route /config
def get_config():
    return jsonify({
        'DATABASE_URL': config.DATABASE_URL,
        'GOOGLE_SHEETS_CREDENTIALS_FILE': config.GOOGLE_SHEETS_CREDENTIALS_FILE,
        'GOOGLE_SHEETS_TOKEN_FILE': config.GOOGLE_SHEETS_TOKEN_FILE,
        'SAMPLE_SPREADSHEET_ID': config.SAMPLE_SPREADSHEET_ID,
        'SAMPLE_RANGE_NAME': config.SAMPLE_RANGE_NAME
    })

if __name__ == '__main__':
    app.run(debug=True)

