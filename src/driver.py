import logging

import create_log_path
import os
import sys
from time import perf_counter
from utils import get_schema_from_json_file, get_source_dataframe
from data_sourcing import get_all_source_df
from data_transformations import get_common_product_set,get_final_transactions,get_sales_agg_by_transactions,\
    get_tax_agg_by_transactions,get_daily_sales_tax_summary,get_amount_by_customer,get_sales_tax_by_products,\
    get_sales_by_tax_brackets,get_sales_by_promotions_membership_level,get_segments_on_expenditure_and_habits,\
    get_sales_tax_by_geographic
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

    transactions_customers_products_promos_prices_returns_df = get_final_transactions(customers_df, promotions_df, products_df, transactions_df, return_products_df)

    logging.info("printing transactions_customers_products_promos_prices_returns_df dataframe")
    transactions_customers_products_promos_prices_returns_df.show()
    transactions_customers_products_promos_prices_returns_df.printSchema()

    #requirement - getting common products that appers together in transactions
    sales_agg_by_transactions_df = get_sales_agg_by_transactions(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_agg_by_transactions_df")
    sales_agg_by_transactions_df.show(truncate=False)
    sales_agg_by_transactions_df.printSchema()

    # requirement - getting the tax amounts aggregation by transactions.
    tax_agg_by_transactions_df = get_tax_agg_by_transactions(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing tax_agg_by_transactions_df")
    tax_agg_by_transactions_df.show(truncate=False)
    tax_agg_by_transactions_df.printSchema()

    # requirement = sales tax agg by day
    sales_tax_agg_by_day = get_daily_sales_tax_summary(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_tax_agg_by_day")
    sales_tax_agg_by_day.show(truncate=False)
    sales_tax_agg_by_day.printSchema()

    # total amount spent by customer
    amount_by_customer_df = get_amount_by_customer(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing amount_by_customer_df")
    amount_by_customer_df.show(truncate=False)
    amount_by_customer_df.printSchema()

    # requirement get sales and tax details by products
    sales_tax_by_products_df = get_sales_tax_by_products(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_tax_by_products_df")
    sales_tax_by_products_df.show(truncate=False)
    sales_tax_by_products_df.printSchema()

    # getting the products that are frequently purchased together
    product_list_df = get_common_product_set(transactions_df)
    logging.info("printing product_list_df dataframe")
    product_list_df.show()
    product_list_df.printSchema()

    # getting sales as per tax slabs
    sales_by_tax_brackets_df = get_sales_by_tax_brackets(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_by_tax_brackets")
    sales_by_tax_brackets_df.show(truncate=False)
    sales_by_tax_brackets_df.printSchema()

    # getting promotional and non-promotional analysis on membership_level

    sales_by_promotions_membership_level = get_sales_by_promotions_membership_level(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_by_promotions_membership_level")
    sales_by_promotions_membership_level.show(truncate=False)
    sales_by_promotions_membership_level.printSchema()

    # getting segmentation on habits and expenditure

    segmenting_on_expenditure_df,segmenting_on_habits_df = get_segments_on_expenditure_and_habits(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing segmenting_on_expenditure_df")
    segmenting_on_expenditure_df.show(truncate=False)
    segmenting_on_expenditure_df.printSchema()
    logging.info("showing segmenting_on_habits_df")
    segmenting_on_habits_df.show(truncate=False)
    segmenting_on_habits_df.printSchema()


    #getting insights by geographic_location

    sales_tax_by_geographic_df = get_sales_tax_by_geographic(transactions_customers_products_promos_prices_returns_df)
    logging.info("showing sales_tax_by_geographic_df")
    sales_tax_by_geographic_df.show(truncate=False)
    sales_tax_by_geographic_df.printSchema()


if __name__ == '__main__':
    main()