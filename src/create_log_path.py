import get_variables as gav
initial_logconf_path = gav.configs['logging_paths']['initial_logconf_path']
running_logconf_path = gav.configs['logging_paths']['running_logconf_path']
running_log_dir = gav.configs['logging_paths']['running_log_dir']
gav.create_log_path(initial_logconf_path,running_logconf_path,running_log_dir)