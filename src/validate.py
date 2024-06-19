from pyspark.sql.functions import *
import logging
import logging.config
from get_variables import get_logconf_file
log_cong_file = get_logconf_file()
logging.config.fileConfig('log_cong_file')
logger = logging.getLogger('Validate')

def validate_spark_session(spark):
    try:
        logger.info('validating spark object')
        checking_spark = spark.sql(""" select current_date """)
        logger.info('validating spark object with the current date {}'.format(checking_spark))

    except Exception as msg:
        logger.error('validation for the spark object has been failed error = {}'.format(msg))

    else:
        logger.info('validation for spark object has been success')
