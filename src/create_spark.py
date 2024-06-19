from pyspark.sql import SparkSession
import logging
import logging.config
import get_variables as gav
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Create_spark')


def get_spark():
    global spark
    try:
        logger.info('started creating spark object')
        spark = SparkSession\
            .builder\
            .appName('Tax_analysis')\
            .master('local[*]')\
            .getOrCreate()
        logger.info('created spark session')
    except Exception as msg:
        logger.error('Can not create spark object due to error == {}'.format(msg))
    return spark


