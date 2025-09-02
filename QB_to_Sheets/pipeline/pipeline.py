from datetime import datetime
from piper.models.pipeline import Pipeline, RUN_IN_ONE_DATACENTER
from piper.tasks.python_task import PythonTask

# Import your task function here (adjust import path after creating tasks file)
from data_piper_core_pipelines.core.gss_piper_bhavneet.QB_to_Sheets.tasks.update_data import update_google_sheets_data


default_args = {
    "owner": "bgill26",
    "schedule_interval": "0 8 * * 3",                 # Weekly schedule, Wed 8 AM
    "trigger_type": "time_interval",
    "start_date": datetime(2025, 3, 24),
    "auto_backfill": False,
    "email": "bgill3@ext.uber.com",
    "email_on_failure": True,
    "owner_ldap_groups": ["daas_eats_delivery"],
    "create_time_uown": '50c39d7b-e6b1-4bf3-81dd-36b67b62fe61'
}

pipeline = Pipeline(
    pipeline_id='update_google_sheets_data',
    default_args=default_args,
    start_date=default_args['start_date'],
    datacenter_choice_mode=RUN_IN_ONE_DATACENTER,
    create_time_uown=default_args['create_time_uown'],
    schedule_interval=default_args['schedule_interval']
)

run_query_and_export_task = PythonTask(
    task_id='task_1',
    python_callable=update_google_sheets_data,
    pipeline=pipeline
)
