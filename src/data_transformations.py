import json
import logging
import logging.config
import get_variables as gav
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import explode, col, collect_set, size, count, when, lit, sum as spark_sum, \
    round as spark_round, when, avg as spark_avg
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

        # Calculating amounts after applying discounts and taxes
        transactions_customers_products_promos_prices = (
            transactions_customers_products_promos_df_joined
            .withColumn("discount_amount", spark_round(col("total_price_before_discount_tax") * col("discount"), 2))
            .withColumn("total_price_after_discount",
                        spark_round(col("total_price_before_discount_tax") - col("discount_amount"), 2))
            .withColumn("tax_amount", spark_round(col("total_price_after_discount") * col("tax_rate"), 2))
            .withColumn("total_price_after_discount_tax", spark_round(col("total_price_after_discount") + col("tax_amount"), 2))
        )

        # checking if the item has been placed for return or not and adjusting the amounts as per returns
        transactions_customers_products_promos_prices_returns_df = (
            transactions_customers_products_promos_prices.join(
                return_products_df,
                (transactions_customers_products_promos_prices.transaction_id == return_products_df.transaction_id) &
                (transactions_customers_products_promos_prices.product_id == return_products_df.return_product_id),
                how="left"
            )
            .withColumn("is_returned", when(col("return_product_id").isNotNull(), True).otherwise(False))
            .withColumn(
                "total_price_after_discount_tax_return_check",
                when(col("is_returned"), 0).otherwise(col("total_price_after_discount_tax"))
            )
            .select(
                transactions_customers_products_promos_prices["*"],
                col("is_returned"),
                col("total_price_after_discount_tax_return_check")
            )
        )

    except Exception as msg:
        logger.error('Error has occurred: {}'.format(msg))
        transactions_customers_products_promos_prices_returns_df = None

    return transactions_customers_products_promos_prices_returns_df


#requirement - get the sales per transactions calculating the total price for items purchased.

def get_sales_agg_by_transactions(final_transaction_df):
    try:
        sales_agg_by_transactions_df = (
            final_transaction_df.groupBy("transaction_id")
            .agg(
                spark_round(spark_sum("total_price_after_discount_tax"), 2)
                .alias("sum_of_total_amount_after_discount_tax"),
                spark_round(spark_sum("total_price_after_discount_tax_return_check"), 2)
                .alias("sum_of_amount_non_returned"),
                spark_round(
                    spark_sum(
                        when(col("is_returned") == True, col("total_price_after_discount_tax"))
                        .otherwise(0)
                    ),2
                )
                .alias("sum_of_amount_returned")

            )
        )

    except Exception as msg:
        logger.error("get_sales_agg_by_transactions has failed to get the aggregated dataframe,ERROR = {}".format(msg))
        sales_agg_by_transactions_df = None

    return sales_agg_by_transactions_df

#requirement - getting the tax amounts aggregation by transactions.
def get_tax_agg_by_transactions(final_transaction_df):
    try:
        tax_agg_by_transactions_df = (
            final_transaction_df.groupBy("transaction_id")
            .agg(
                spark_round(
                    spark_sum(
                        when(col("is_returned") == False, col("tax_amount"))
                        .otherwise(0)
                    ),2
                )
                .alias("non_returned_products_tax_amount"),
                spark_round(
                    spark_sum(
                        when(col("is_returned") == True, col("tax_amount"))
                        .otherwise(0)
                    ),2
                )
                .alias("returned_products_tax_amount")
            )
        )

    except Exception as msg:
        logger.error("function has failed to create tax_agg_by_transactions_df ERROR = {}".format(msg))
        tax_agg_by_transactions_df = None

    return tax_agg_by_transactions_df



#requirement = sales tax agg by day
def get_daily_sales_tax_summary(final_transaction_df):
    try:
        sales_tax_agg_by_day = (
            final_transaction_df.groupBy("date")
            .agg(
                spark_round(
                    spark_sum(
                        when(col("is_returned") == False, col("tax_amount"))
                        .otherwise(0)
                    ), 2
                ).alias("non_returned_products_tax_amount"),
                spark_round(
                    spark_sum(
                        when(col("is_returned") == True, col("tax_amount"))
                        .otherwise(0)
                    ), 2
                ).alias("returned_products_tax_amount"),
                spark_round(spark_sum("total_price_after_discount_tax"), 2)
                .alias("sum_of_total_amount_after_discount_tax"),
                spark_round(spark_sum("total_price_after_discount_tax_return_check"), 2)
                .alias("sum_of_amount_non_returned"),
                spark_round(
                    spark_sum(
                        when(col("is_returned") == True, col("total_price_after_discount_tax"))
                        .otherwise(0)
                    ), 2
                ).alias("sum_of_amount_returned")
            )
        )

    except Exception as msg:
        logger.error("function get_daily_sales_tax_summary has failed ERROR = {}".format(msg))
        sales_tax_agg_by_day = None

    return sales_tax_agg_by_day

#total amount spent by custoemr
def get_amount_by_customer(final_transaction_df):
    try:
        amount_by_customer_df = (
            final_transaction_df.groupBy("customer_id","year")
            .agg(
                spark_round(spark_sum("total_price_after_discount_tax_return_check"), 2)
                .alias("sum_of_amount_spent_by_customer")
            )
        )

    except Exception as msg:
        logger.info("funtion get_amount_by_customer has failed ERROR: {}".format(msg))
        amount_by_customer_df = None

    return amount_by_customer_df

#requirement get sales and tax details by products
def get_sales_tax_by_products(final_transaction_df):
    try:
        sales_tax_by_products_df = (
            final_transaction_df.groupBy("product_id")
            .agg(
                spark_round(spark_sum("total_price_after_discount_tax_return_check"), 2)
                .alias("total_amount_per_products"),
                spark_round(
                    spark_sum(
                        when(col("is_returned") == False, col("tax_amount"))
                        .otherwise(0)
                    ), 2
                ).alias("non_returned_products_tax_amount")
            )
        )

    except Exception as msg:
        logger.error("fucntion get_sales_tax_by_products has been failed Error : {}".format(msg))
        sales_tax_by_products_df = None

    return sales_tax_by_products_df

# requirement - getting common products that appers together in transactions
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

# requirement - getting sales by tax brackets
def get_sales_by_tax_brackets(final_transaction_df):
    try:
        sales_by_tax_brackets_df = (
            final_transaction_df
            .withColumn(
                "tax_brackets",
                when((col("tax_rate") >= 0) & (col("tax_rate") <= 0.1), "0 - 0.1")
                .when((col("tax_rate") > 0.1) & (col("tax_rate") <= 0.2), "0.11 - 0.2")
                .otherwise('others')
            )
            .groupBy("tax_brackets")
            .agg(
                spark_round(spark_sum("total_price_after_discount_tax_return_check"), 2)
                .alias("total_sales_by_tax_brackets")
            )
        )

    except Exception as e:
        logger.error(f"Error in calculating sales by tax brackets: {e}")
        sales_by_tax_brackets_df =  None

    return sales_by_tax_brackets_df


# requirement - getting promotional and non promotional analysis by membership_level

def get_sales_by_promotions_membership_level(final_transaction_df):
    try:
        sales_by_promotions_membership_level = (
            final_transaction_df.groupBy("membership_level")
            .agg(
                spark_round(
                    spark_sum(
                        when(col("discount") > 0, col("total_price_after_discount_tax_return_check"))
                        .otherwise(0)
                    ),2
                )
                .alias('total_sales_with_promotions'),
                spark_round(
                    spark_sum(
                        when(col("discount") == 0, col("total_price_after_discount_tax_return_check"))
                        .otherwise(0)
                    ),2
                )
                .alias("total_sales_without_promotions")

            )
        )

    except Exception as msg:
        logger.error("function get_sales_by_promotions_membership_level has failed with error: {}".format(msg))
        sales_by_promotions_membership_level = None

    return sales_by_promotions_membership_level

def get_segments_on_expenditure_and_habits(final_transaction_df):
    try:
        segmenting_on_expenditure_df = (
            final_transaction_df.groupBy("customer_id","year")
            .agg(
                spark_round(
                    spark_avg("total_price_after_discount_tax_return_check"),2
                )
                .alias("avg_customer_expenditure_yearly")
            )
        )

    except Exception as msg:
        logger.info("the segmenting_on_expenditure_df has failed ERROR : {}".format(msg))
        segmenting_on_expenditure_df = None

    try:
        segmenting_on_habits_df = (
            final_transaction_df.withColumn(
                "brand_of_product", col("attribute.brand")
            )
            .groupBy("customer_id","brand_of_product")
            .agg(
                count(col("product_id")).alias("no_of_times_customer_purchased_brand")
            )
            .orderBy(col('no_of_times_customer_purchased_brand').desc())
        )

    except Exception as msg:
        logger.info("the segmenting_on_habits_df has failed ERROR : {}".format(msg))
        segmenting_on_habits_df = None

    return (segmenting_on_expenditure_df,segmenting_on_habits_df)
