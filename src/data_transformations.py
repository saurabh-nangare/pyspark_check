import json
import logging
import logging.config
import get_variables as gav
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode, col, collect_set, size, count, when, lit, sum as spark_sum, \
    round as spark_round

# Configuring the logger for Data transformations functions
logging.config.fileConfig(gav.configs['logging_paths']['running_logconf_path'])
logger = logging.getLogger('Data_transformations')


def get_final_transactions(customers_df, promotions_df, products_df, transactions_df, return_products_df):
    """
    We are trying to get the final transactions dataframe.
    :return: final dataframe for transactions
    """
    try:
        logger.info("Started calculating the final transactions dataframe")

        # getting prodcuts and their quantity out from transactions dataframe
        transactions_df_exploded = (
            transactions_df.withColumn("item", explode("items"))
            .select(
                "transaction_id",
                "customer_id",
                "date",
                "year",
                "item.product_id",
                "item.quantity"
            )
        )

        # Joining transactions with customers to get customer information
        transactions_customers_df_joined = transactions_df_exploded.join(customers_df, "customer_id")

        # Joining with products to get product details
        transactions_customers_products_df_joined = (
            transactions_customers_df_joined.join(products_df, "product_id")
            .withColumn("total_price_before_discount_tax", col("price") * col("quantity"))
        )

        # Join with promotions to decide if the discount will be applicable or not based on membership level
        transactions_customers_products_promos_df_joined = (
            transactions_customers_products_df_joined.join(
                promotions_df,
                (transactions_customers_products_df_joined.product_id == promotions_df.applicable_products) &
                (transactions_customers_products_df_joined.membership_level == promotions_df.membership_level),
                "left"
            )
            .select(
                'transaction_id',
                'customer_id',
                'product_id',
                'date',
                'year',
                'quantity',
                'name',
                transactions_customers_products_df_joined['membership_level'],
                'geographic_region',
                'purchase_history',
                transactions_customers_products_df_joined['description'],
                'attribute',
                'price',
                'tax_rate',
                'total_price_before_discount_tax',
                when(promotions_df.discount.isNull(), lit(0)).otherwise(promotions_df.discount).alias("discount")
            )
        )

        # Calculate prices after discount and tax
        transactions_customers_products_promos_prices = (
            transactions_customers_products_promos_df_joined
            .withColumn("discount_amount", spark_round(col("total_price_before_discount_tax") * col("discount"), 2))
            .withColumn("total_price_after_discount",
                        spark_round(col("total_price_before_discount_tax") - col("discount_amount"), 2))
            .withColumn("tax_amount", spark_round(col("total_price_after_discount") * col("tax_rate"), 2))
            .withColumn("total_price_after_tax", spark_round(col("total_price_after_discount") + col("tax_amount"), 2))
        )

        # Handle returns
        transactions_customers_products_promos_prices_returns_df = (
            transactions_customers_products_promos_prices.join(
                return_products_df,
                (transactions_customers_products_promos_prices.transaction_id == return_products_df.transaction_id) &
                (transactions_customers_products_promos_prices.product_id == return_products_df.return_product_id),
                how="left"
            )
            .withColumn("is_returned", when(col("return_product_id").isNotNull(), True).otherwise(False))
            .withColumn(
                "total_price_after_tax_after_return_check",
                when(col("is_returned"), 0).otherwise(col("total_price_after_tax"))
            )
            .select(
                transactions_customers_products_promos_prices["*"],
                col("is_returned"),
                col("total_price_after_tax_after_return_check")
            )
        )

    except Exception as msg:
        logger.error('Error has occurred: {}'.format(msg))
        transactions_customers_products_promos_prices_returns_df = None

    return transactions_customers_products_promos_prices_returns_df


def get_common_product_set(transactions_df):
    try:
        logger.info("Started calculating common sets of products that appear together")
        product_list_df = (
            transactions_df.withColumn("item", explode(col("items")))
            .withColumn("product_id", col("item.product_id"))
            .groupBy("transaction_id")
            .agg(collect_set("product_id").alias("product_list"))
            .filter(size(col("product_list")) > 1)
            .groupBy("product_list")
            .agg(count("*").alias("no_of_times_group_of_products_purchased_together"))
            .orderBy(col("no_of_times_group_of_products_purchased_together").desc())
        )

    except Exception as msg:
        logger.error("Cannot create product_list_df due to error: {}".format(msg))
        product_list_df = None

    return product_list_df
