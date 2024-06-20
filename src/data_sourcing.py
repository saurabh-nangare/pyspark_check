import json
import logging
import logging.config
import get_variables as gav
from utils import get_schema_from_json_file,get_source_dataframe
from pyspark.sql.functions import explode, year, col

#setting up the logging for data_sourcing module
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Data_sourcing')


def get_all_source_df(spark):
    try:
        customers_source_path = gav.get_source_paths('customers')
        customers_schema_path = gav.get_schema_path('customers')
        customers_schema = get_schema_from_json_file(spark, customers_schema_path)
        customers_df = get_source_dataframe(spark, customers_schema, customers_source_path)

    except Exception as msg:
        logger.error("can not create customers dataframe, error occured = {}".format(msg))
        customers_df = None
    try:
        promotions_source_path = gav.get_source_paths('promotions')
        promotions_schema_path = gav.get_schema_path('promotions')
        promotions_schema = get_schema_from_json_file(spark, promotions_schema_path)
        promotions_df = get_source_dataframe(spark, promotions_schema, promotions_source_path)
        promotions_df = promotions_df.withColumn('applicable_products', explode('applicable_products'))

    except Exception as msg:
        logger.error("can not create promotions dataframe, error occured = {}".format(msg))
        promotions_df = None

    try:
        products_source_path = gav.get_source_paths('products')
        products_schema_path = gav.get_schema_path('products')
        products_schema = get_schema_from_json_file(spark, products_schema_path)
        products_df = get_source_dataframe(spark, products_schema, products_source_path)

    except Exception as msg:
        logger.error("can not create products dataframe, error occured = {}".format(msg))
        products_df = None

    try:
        transactions_source_path = gav.get_source_paths('transactions')
        transactions_schema_path = gav.get_schema_path('transactions')
        transactions_schema = get_schema_from_json_file(spark, transactions_schema_path)
        transactions_df = get_source_dataframe(spark, transactions_schema, transactions_source_path)
        transactions_df = transactions_df.withColumn("year", year(col("date")))

    except Exception as msg:
        logger.error("can not create transactions dataframe, error occured = {}".format(msg))
        transactions_df = None

    try:
        return_products_source_path = gav.get_source_paths('return_products')
        return_products_schema_path = gav.get_schema_path('return_products')
        return_products_schema = get_schema_from_json_file(spark, return_products_schema_path)
        return_products_df = get_source_dataframe(spark, return_products_schema, return_products_source_path)

    except Exception as msg:
        logger.error("can not create transactions dataframe, error occured = {}".format(msg))
        return_products_df = None

    return (customers_df, promotions_df, products_df, transactions_df, return_products_df)