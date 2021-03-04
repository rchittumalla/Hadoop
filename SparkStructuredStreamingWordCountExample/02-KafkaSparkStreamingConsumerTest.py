# coding: utf-8

# ## Initializing Spark 
# Create __SparkSession__ object
# 
#     The entry point to programming Spark with the Dataset and DataFrame API.
# 
#     Used to create DataFrame, register DataFrame as tables and execute SQL over tables etc.

from pyspark.sql import SparkSession

# Import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Kafka Spark Structured Streaming") \
    .config("spark.master", "local") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "c.insofe.edu.in:9092") \
  .option("subscribe", "insofe_topic_batch43") \
  .option("startingOffsets", "earliest") \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.printSchema()

df = df.select(col("key").cast("string"), col("value").cast("string"))

df.printSchema()

query = df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate","false") \
        .start()

query.awaitTermination()
