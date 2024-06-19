from datetime import datetime
import os
import configparser
configs = configparser.ConfigParser()
configs.read(r'C:\Users\saura\Desktop\pyspark_check\properties\configurations\project_configs.ini')

def create_log_path(initial_logconf_path,running_logconf_path, running_log_dir):
    # Generate the dynamic filename with an absolute path
    run_start_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = running_log_dir  # Replace with the desired directory
    log_filename = f'{run_start_time}_tax_analysis.log'
    log_filepath = os.path.join(log_dir, log_filename)
    config = configparser.ConfigParser()
    config.read(initial_logconf_path)
    config.set('handler_fileHandler', 'args', f"({repr(log_filepath)},)")
    with open(running_logconf_path,
              'w') as configfile:
        config.write(configfile)


def get_logconf_file():
    return configs['logging_paths']['running_logconf_path']

def get_source_paths(source_name):
    # global configs
    return configs['source_paths'][f'{source_name}' + '_source']

def get_schema_path(source_name):
    return configs['schema_paths'][f'{source_name}_schema']











