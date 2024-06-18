from pyspark.sql.functions import *


def validate_spark_session(spark):
    try:
        print('validating spark object')
        checking_spark = spark.sql(""" select current_date """)
        print('validating spark object with the current date {}'.format(checking_spark))

    except Exception as msg:
        print('validation for the spark object has been failed error = {}'.format(msg))

    else:
        print('validation for spark object has been success')
