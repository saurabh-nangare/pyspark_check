import create_log_path
import os
import sys
from time import perf_counter
from utils import get_schema_from_json_file, get_source_dataframe
import get_variables as gav
from create_spark import get_spark
import logging.config
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])


def main():
    logging.info("creating spark session")
    spark = get_spark()
    print(spark)
    logging.info("spark session has been created successfully")
    logging.info('trying to read source dataframe for cutomers')
    logging.info('trying to get schema for customer')

    customers_source_path = gav.get_source_paths('customers')
    customers_schema_path = gav.get_schema_path('customers')
    customers_schema = get_schema_from_json_file(spark,customers_schema_path)
    customers_df = get_source_dataframe(spark,customers_schema,customers_source_path)
    customers_df.show()
    customers_df.printSchema()

    promotions_source_path = gav.get_source_paths('promotions')
    promotions_schema_path = gav.get_schema_path('promotions')
    promotions_schema = get_schema_from_json_file(spark, promotions_schema_path)
    promotions_df = get_source_dataframe(spark, promotions_schema, promotions_source_path)
    promotions_df.show()
    promotions_df.printSchema()

    products_source_path = gav.get_source_paths('products')
    products_schema_path = gav.get_schema_path('products')
    products_schema = get_schema_from_json_file(spark, products_schema_path)
    products_df = get_source_dataframe(spark, products_schema, products_source_path)
    products_df.show()
    products_df.printSchema()

    transactions_source_path = gav.get_source_paths('transactions')
    transactions_schema_path = gav.get_schema_path('transactions')
    transactions_schema = get_schema_from_json_file(spark, transactions_schema_path)
    transactions_df = get_source_dataframe(spark, transactions_schema, transactions_source_path)
    transactions_df.show()
    transactions_df.printSchema()

    return_products_source_path = gav.get_source_paths('return_products')
    return_products_schema_path = gav.get_schema_path('return_products')
    return_products_schema = get_schema_from_json_file(spark, return_products_schema_path)
    return_products_df = get_source_dataframe(spark, return_products_schema, return_products_source_path)
    return_products_df.show()
    return_products_df.printSchema()


if __name__ == '__main__':
    main()