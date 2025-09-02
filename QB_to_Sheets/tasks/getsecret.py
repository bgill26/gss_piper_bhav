import os
import pygsheets
from clay import config
from piper.utils import config_helper
import logging
import json
from pipelines.core.gss_piper_bhavneet.utils.config import get_pipeline_config
from google.oauth2 import service_account


logging = config.get_logger('piper')

SCOPES = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

def getsecret(secret,dict):


        secret = config.get("bhavgill-dsw-gcp-secret")

        if isinstance(secret, dict):
            creds_blob = secret.get('creds')
        else:
            creds_blob = secret

        if isinstance(creds_blob, str) and os.path.exists(creds_blob):
            with open(creds_blob, 'r') as f:
                creds_json = json.load(f)
        else:
            if isinstance(creds_blob, str):
                creds_json = json.loads(creds_blob)
            elif isinstance(creds_blob, dict):
                creds_json = creds_blob
            else:
                raise ValueError("creds_blob is not a string or dict")

        credentials = service_account.Credentials.from_service_account_info(
            creds_json,
            scopes=SCOPES
        )

        gc = pygsheets.authorize(custom_credentials=credentials)
        logging.info("Successfully authorized pygsheets using the specified secret.")
        return gc
