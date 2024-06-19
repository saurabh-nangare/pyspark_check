import create_log_path
import os
import sys
from time import perf_counter
from utils import get_schema_from_json_file, get_customers_dataframe
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
    # schema_customers = get_schema_from_json_file(spark,
    #                                              r'/properties/source_file_schema/customers_schema/schema_customers.json')
    #
    # print(schema_customers)
    # customer_df = get_customers_dataframe(spark, schema_customers, r'C:\\Users\\saura\\Desktop\\pyspark_check\\landing_zone\\customers\\customers.json' )
    #
    # customer_df.show()
    # customer_df.printSchema()
    schema_transactions = get_schema_from_json_file(spark,
                                                 r'C:\Users\saura\Desktop\pyspark_check\properties\source_file_schema\transactions_schema\schema_transactions.json')
    transactions_df = get_customers_dataframe(spark,schema_transactions , r'C:\Users\saura\Desktop\pyspark_check\landing_zone\transactions\transactions.json')

    transactions_df.show()
    transactions_df.printSchema()

if __name__ == '__main__':
    main()