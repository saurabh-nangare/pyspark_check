from pyspark.sql import SparkSession

def get_spark():
    global spark
    try:
        spark = SparkSession\
            .builder\
            .appName('Tax_analysis')\
            .master('local[*]')\
            .getOrCreate()

    except Exception as msg:
        print('Can not create spark object due to error == {}'.format(msg))
     return spark


