import os
import pandas as pd
import pygsheets
from queryrunner_client import Client
from clay import config
from piper.utils import config_helper
import logging

logging = config.get_logger('piper')

# Relative path to your SQL query file from pipeline base
SQL_QUERY_REL_PATH = os.path.join('pipelines', 'core', 'gss_piper_chinmay', 'QB_to_Sheets', 'SQL_files', 'anz_jira.sql')

SPREADSHEET_ID = '14FyRAEXhYp8YRVVUyAZ3gy6-xiTaoLhRBEuMXiZ-Vz8'
WORKSHEET_NAME = 'Sheet1'

def run_query_and_export_to_gsheet():
    try:
        base_path = config_helper.get_pipelines_base_path()
        sql_query_path = os.path.join(base_path, SQL_QUERY_REL_PATH)

        with open(sql_query_path, 'r') as sql_file:
            sql_query = sql_file.read()

        qr = Client(user_email='csharm35@ext.uber.com', interactive=False)
        cursor = qr.execute('presto', sql_query, timeout=7200)
        df = pd.DataFrame(cursor.fetchall(), columns=cursor.columns)
        logging.info(f"SQL query returned {df.shape[0]} rows and {df.shape[1]} columns")

        # --- Directly get your GCP secret from clay here ---
        secret = config.get("csharm35-dsw-gcp-secret")

        # Determine creds path (handle dict or str, depending on secret format)
        if isinstance(secret, dict):
            creds_path = secret.get('creds')
        elif isinstance(secret, str):
            creds_path = secret
        else:
            raise ValueError(f"Unexpected secret format: {type(secret)}")

        if not creds_path or not os.path.exists(creds_path):
            raise FileNotFoundError(f"Credentials file not found at path: {creds_path}")

        # Authorize pygsheets directly with the creds_path
        gc = pygsheets.authorize(service_account_file=creds_path)

        # Insert data
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet_by_title(WORKSHEET_NAME)
        worksheet.clear(start='A2', end='Z1000000')
        worksheet.set_dataframe(df, start='A2', copy_head=False, value_input_option='USER_ENTERED')

        logging.info("Data successfully exported to Google Sheets.")

    except Exception as e:
        logging.error(f"Error running SQL and exporting to Google Sheets: {e}", exc_info=True)
        raise
