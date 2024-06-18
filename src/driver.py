
from get_variables import create_log_path
import configparser
configs = configparser.ConfigParser()
configs.read(r'C:\Users\saura\Desktop\pyspark_check\properties\configurations\project_configs.ini')
a = configs['logging_paths']['initial_logconf_path']
b = configs['logging_paths']['running_logconf_path']
c = configs['logging_paths']['running_log_dir']

print(a)
print(b)
print(c)
# create_log_path(initial_logconf_path=a,running_logconf_path=b,running_log_dir=c)