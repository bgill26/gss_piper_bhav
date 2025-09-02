import pandas as pd
import os
import re # Added for regular expression matching
from queryrunner_client import Client
import pandas_gbq
import requests
import json
import pygsheets
import gspread
from clay import config
from piper.utils import config_helper
import logging
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativela import relativedelta
from query_runner_library import QueryRunner
from query_runner_library import QueryRunnerException
from data_piper_core_pipelines.core.gss_piper_bhavneet.utils.gsheet import load_credentials
import getpass


scope = ['https://www.googleapis.com/auth/spreadsheets.readonly',
         'https://www.googleapis.com/auth/drive',
         'https://www.googleapis.com/auth/spreadsheets',
         'https://spreadsheets.google.com/feeds'
         ]

qr = QueryRunner()
logging = config.get_logger('piper')

# CONFIG FILE PATH
GCP_CREDENTIALS_CONFIG_REL_PATH = os.path.join(
    'QB_to_sheets', 'config', 'config.yaml'
)


# Dictionary to map report names to their IDs
REPORTS_MAP = {
    'gm_canada_df': '5xOLBGTNV',
    'ue_category_df': 'EBBjRwg9R',
    'ue_df': 'EcD2hvNC3',
    'bars_cat_df': '6EI1X4iXZ',
    'cereal_cat_df': '5iL75DTKv',
    'fruit_cat_df': 'wNXCY5ni7',
    'yogurt_df': 'Ie7Ovb7ld',
    'groc_df': 'tZAHpF49h',
    'conv_df': '26baAPipl',
    'gm_yogurt_df': '2fUcFAusv',
    'gm_cereal_df': 'xSVjhv0o7',
    'gm_fruit_df': 'DUuolLf0X',
    'gm_bars_df': 'pfBhmRjzh',
    'gm_conv_df': 'hhhykk5Z5',
    'gm_groc_df': 'HRVj78ED'
}

# Dictionary to map each DataFrame to its destination in the Google Sheet
DATAFRAME_DESTINATIONS = {
    'GM Sales': {'df_name': 'gm_canada_df', 'cell': 'B3', 'worksheet_name': 'GM Sales'},
    'UE Sales': {'df_name': 'ue_df', 'cell': 'B13', 'worksheet_name': 'UE Sales'},
    'UE Sales - Category': {'df_name': 'ue_category_df', 'cell': 'L13', 'worksheet_name': 'UE Sales'},
    'Category Summary - Bars': {'df_name': 'bars_cat_df', 'cell': 'B8', 'worksheet_name': 'Category Summary'},
    'Category Summary - Cereal': {'df_name': 'cereal_cat_df', 'cell': 'B14', 'worksheet_name': 'Category Summary'},
    'Category Summary - Fruit': {'df_name': 'fruit_cat_df', 'cell': 'B20', 'worksheet_name': 'Category Summary'},
    'Category Summary - Yogurt': {'df_name': 'yogurt_df', 'cell': 'B26', 'worksheet_name': 'Category Summary'},
    'Category Summary - Grocery': {'df_name': 'groc_df', 'cell': 'G8', 'worksheet_name': 'Category Summary'},
    'Category Summary - Convenience': {'df_name': 'conv_df', 'cell': 'G14', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Grocery': {'df_name': 'gm_groc_df', 'cell': 'B39', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Convenience': {'df_name': 'gm_conv_df', 'cell': 'G39', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Bars': {'df_name': 'gm_bars_df', 'cell': 'L39', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Cereal': {'df_name': 'gm_cereal_df', 'cell': 'Q39', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Fruit': {'df_name': 'gm_fruit_df', 'cell': 'V39', 'worksheet_name': 'Category Summary'},
    'Category Summary - GM Yogurt': {'df_name': 'gm_yogurt_df', 'cell': 'AA39', 'worksheet_name': 'Category Summary'}
}


def get_reports_data(qr_instance: QueryRunner, reports: dict) -> dict:
    """
    Fetches data from QueryRunner reports and returns a dictionary of DataFrames.
    """
    results_dfs = {}
    for name, report_id in reports.items():
        try:
            df = pd.DataFrame(qr_instance.execute_report(report_id, timeout=6000).load_data())
            # Convert datetime columns to string to prevent writing issues with pygsheets
            datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns
            if not datetime_cols.empty:
                df[datetime_cols] = df[datetime_cols].astype(str)
                logging.info(f"Converted datetime columns to string for {name}.")
            results_dfs[name] = df
            logging.info(f"Successfully fetched and processed data for report ID: {report_id}")
        except QueryRunnerException as e:
            logging.error(f"Failed to fetch data for report ID {report_id}: {e}")
            raise  # Re-raise the exception to stop the script
    return results_dfs


def update_google_sheets_data(sheet_key: str):
    """
    Fetches data from various QueryRunner reports and updates a Google Sheet.
    """
    try:
        # Load base path and authenticate
        base_path = config_helper.get_pipelines_base_path()
        # Authenticate with Google Sheets using a service account file
        gc = load_credentials(GCP_CREDENTIALS_CONFIG_REL_PATH)
        logging.info("Successfully authenticated with Google Sheets.")

        # Open the specified Google Sheet
        sheet = gc.open_by_key(sheet_key)
        print(f"Successfully opened Google Sheet: {sheet.title}")

    except Exception as e:
        logging.error(f"Google Sheets authentication or sheet opening failed: {e}", exc_info=True)
        # Re-raise the exception to stop the script if authentication or sheet opening fails
        raise Exception("Google Sheets authentication or sheet opening failed.")

    try:
        # Fetch all data from QueryRunner reports
        reports_data = get_reports_data(qr, REPORTS_MAP)
        print("All required data fetched from reports.")

        # --- Overwriting Data in Google Sheets using the mapping dictionary ---
        print("Overwriting data in Google Sheets...")

        # Process each destination in the dictionary
        for task_name, mapping in DATAFRAME_DESTINATIONS.items():
            try:
                # 4. Define start cell mapping dynamically using the task_name
                worksheet_name = mapping['worksheet_name']
                start_cell = mapping['cell']

                # Parse the start_cell string into column letters and row number
                match = re.match(r"^([A-Z]+)([0-9]+)$", start_cell)
                if not match:
                    raise ValueError(f"Invalid start_cell format returned: {start_cell}")

                col_letters, row_num = match.groups()
                start_row = int(row_num)

                # Convert column letters (A, B, AA) into a 1-based integer index
                start_col = 0
                for char in col_letters:
                    start_col = start_col * 26 + (ord(char) - 64)

                logging.info(
                    f"Using worksheet '{worksheet_name}', "
                    f"start_cell {start_cell} (row {start_row}, col {start_col})"
                )

                # Use the extracted information to select the worksheet
                worksheet = sheet.worksheet_by_title(worksheet_name)

                # Get the DataFrame to write from the reports_data dictionary
                df_to_write = reports_data[mapping['df_name']]

                # Write the DataFrame to the correct worksheet and cell
                # Note: set_dataframe can accept the cell string, so the parsing is for logging/other uses.
                worksheet.set_dataframe(df_to_write, start_cell, extend=True, copy_head=False)
                print(f"Updated '{worksheet_name}' tab at cell {start_cell}.")

            except pygsheets.exceptions.WorksheetNotFound as e:
                logging.error(f"Worksheet not found for title '{worksheet_name}': {e}")
            except KeyError as e:
                logging.error(f"DataFrame or cell mapping error for '{task_name}': Missing key {e}")
            except Exception as e:
                logging.error(f"An error occurred while updating '{task_name}': {e}", exc_info=True)

        print("All specified Google Sheets tabs updated successfully!")

    except QueryRunnerException as e:
        logging.error(f"QueryRunner Error: A problem occurred while executing a query. {e}", exc_info=True)
    except Exception as e:
        logging.error(f"An unexpected error occurred during data processing or writing: {e}", exc_info=True)


if __name__ == '__main__':
    # Define the spreadsheet ID
    SPREADSHEET_ID = '1LYwBcHnqT-yc8r1EEnxCjI3iB4Lw7nEdvxMO-FTou4Y'
    update_google_sheets_data(SPREADSHEET_ID)
