from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv(r'D:\Hu_Edge\dataset.txt',header=True,sep=',')
df.write.parquet(r'D:\Hu_Edge\datawritecheck',mode= 'append')

print('git')