import os
import pygsheets
from clay import config
from piper.utils import config_helper
import logging

logging = config.get_logger('piper')

def load_credentials(config_file_relative_path):
    try:
        base_path = config_helper.get_pipelines_base_path()
        config_full_path = os.path.join(base_path, config_file_relative_path)

        import yaml
        with open(config_full_path, 'r') as f:
            pipeline_config = yaml.safe_load(f)

        gcp_secret_key = pipeline_config.get('gcp_secret_key')
        if not gcp_secret_key:
            raise ValueError("gcp_secret_key not found in pipeline config")

        logging.info(f"gcp_secret_key : {gcp_secret_key}")
        secret_path = config.get(gcp_secret_key)

        if isinstance(secret_path, dict):
            creds_path = secret_path.get('creds')
        elif isinstance(secret_path, str):
            creds_path = secret_path
        else:
            raise ValueError(f"Unexpected secret_path type: {type(secret_path)}")

        if not creds_path or not os.path.exists(creds_path):
            raise FileNotFoundError(f"Credentials file does not exist at path: {creds_path}")

        gc = pygsheets.authorize(service_account_file=creds_path)
        return gc

    except Exception as e:
        logging.error(f"Unable to get credentials to Google service account. Caused by error: {e}")
        raise

def insert_data_in_google_sheets(gc, spreadsheet_id, dataframe, worksheet_name):
    """
    Insert and overwrite data in Google Sheets tab from a pandas DataFrame.

    Args:
        gc (pygsheets.Client): Authorized Google Sheets client
        spreadsheet_id (str): ID of Google Sheet
        dataframe (pandas.DataFrame): DataFrame to write
        worksheet_name (str): Sheet/tab name in the spreadsheet
    """
    try:
        spreadsheet = gc.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet_by_title(worksheet_name)

        # Clear the range starting from A2 to Z and row 1,000,000 (mimics clearing table content)
        worksheet.clear(start='A2', end='Z1000000')

        # Write the DataFrame to the sheet starting at cell A2 without the header
        worksheet.set_dataframe(
            dataframe,
            start='A2',
            copy_head=False,
            value_input_option='USER_ENTERED'
        )
        logging.info(f"{len(dataframe) * len(dataframe.columns)} cells updated in Google Sheet [{worksheet_name}]")

    except Exception as e:
        logging.error(f"Error updating Google Sheet: {e}")
        raise
