import create_log_path
import os
import sys
from time import perf_counter
from utils import get_schema_from_json_file, get_source_dataframe,get_all_source_df
from data_transformations import get_common_product_set,get_final_transactions
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

    (customers_df, promotions_df, products_df, transactions_df, return_products_df) = get_all_source_df(spark)
    customers_df.show()
    customers_df.printSchema()

    promotions_df.show()
    promotions_df.printSchema()

    products_df.show()
    products_df.printSchema()

    transactions_df.show()
    transactions_df.printSchema()

    return_products_df.show()
    return_products_df.printSchema()

    product_list_df = get_common_product_set(transactions_df)

    logging.info("printing product_list_df dataframe")
    product_list_df.show()
    product_list_df.printSchema()

    transactions_customers_products_promos_prices_returns_df = get_final_transactions(customers_df, promotions_df, products_df, transactions_df, return_products_df)

    logging.info("printing transactions_customers_products_promos_prices_returns_df dataframe")
    transactions_customers_products_promos_prices_returns_df.show()
    transactions_customers_products_promos_prices_returns_df.printSchema()


if __name__ == '__main__':
    main()