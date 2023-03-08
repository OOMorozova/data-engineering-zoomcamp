import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('/Users/omorozova/BigData/data-engineering-zoomcamp/week_1_Docker_Postgres/taxi+_zone_lookup.csv')

df.show()
df.write.parquet('zones')
