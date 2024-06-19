import json
import logging
import logging.config
import get_variables as gav
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Utils')


def get_schema_from_json_file(spark,schema_file_path):
    json_schema = None
    try:
        logger.info("trying to get schema for {}".format(schema_file_path))
        json_schema = spark.read.text(schema_file_path).collect()
    except Exception as e:
        logger.info('can not read the schema file because of error {}'.format(e))
    else:
        json_schema_string = '\n'.join([i.value for i in json_schema])
        json_schema = StructType.fromJson(json.loads(json_schema_string))
        logger.info('schema has beeen created for{}'.format(schema_file_path))
    return json_schema


def get_source_dataframe(spark, schema, source_file_path):
    try:
        logger.info("trying to create dataframe for{}".format(source_file_path))
        source_df = (spark.read
                     .schema(schema)
                     .json(source_file_path))
    except Exception as msg:
        logger.info('Can not source the dataframe because of this error == {}'.format(msg))
        source_df = None

    else:
        logger.info('dataframe has been created for {}'.format(source_file_path))
        logger.info('dropping null records if any for {}'.format(source_file_path))
        source_df = source_df.na.drop()
        logger.info('dropping null records if any has been completed for {}'.format(source_file_path))
    return source_df


def get_all_source_df(spark):
    try:
        customers_source_path = gav.get_source_paths('customers')
        customers_schema_path = gav.get_schema_path('customers')
        customers_schema = get_schema_from_json_file(spark, customers_schema_path)
        customers_df = get_source_dataframe(spark, customers_schema, customers_source_path)

    except Exception as msg:
        logger.error("can not create customers dataframe, error occured = {}".format(msg))

    try:
        promotions_source_path = gav.get_source_paths('promotions')
        promotions_schema_path = gav.get_schema_path('promotions')
        promotions_schema = get_schema_from_json_file(spark, promotions_schema_path)
        promotions_df = get_source_dataframe(spark, promotions_schema, promotions_source_path)

    except Exception as msg:
        logger.error("can not create promotions dataframe, error occured = {}".format(msg))

    try:
        products_source_path = gav.get_source_paths('products')
        products_schema_path = gav.get_schema_path('products')
        products_schema = get_schema_from_json_file(spark, products_schema_path)
        products_df = get_source_dataframe(spark, products_schema, products_source_path)

    except Exception as msg:
        logger.error("can not create products dataframe, error occured = {}".format(msg))

    try:
        transactions_source_path = gav.get_source_paths('transactions')
        transactions_schema_path = gav.get_schema_path('transactions')
        transactions_schema = get_schema_from_json_file(spark, transactions_schema_path)
        transactions_df = get_source_dataframe(spark, transactions_schema, transactions_source_path)

    except Exception as msg:
        logger.error("can not create transactions dataframe, error occured = {}".format(msg))

    try:
        return_products_source_path = gav.get_source_paths('return_products')
        return_products_schema_path = gav.get_schema_path('return_products')
        return_products_schema = get_schema_from_json_file(spark, return_products_schema_path)
        return_products_df = get_source_dataframe(spark, return_products_schema, return_products_source_path)

    except Exception as msg:
        logger.error("can not create transactions dataframe, error occured = {}".format(msg))

    return (customers_df, promotions_df, products_df, transactions_df, return_products_df)



