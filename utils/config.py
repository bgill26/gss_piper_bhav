import os
import yaml
from piper.utils import config_helper

def get_pipeline_config(config_file_name):

    base_path = config_helper.get_pipelines_base_path()

    pipeline_config_file_path = os.path.join(base_path, config_file_name)

    with open(pipeline_config_file_path, encoding='utf-8') as f:
        pipeline_config = yaml.safe_load(f)

    return pipeline_config
