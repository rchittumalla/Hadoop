# Create __SparkSession__ object
# 
#     The entry point to programming Spark with the Dataset and DataFrame API.
# 
#     Used to create DataFrame, register DataFrame as tables and execute SQL over tables etc.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Kafka Spark Structured Streaming") \
    .config("spark.master", "local") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")	
	
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "c.insofe.edu.in:9092") \
  .option("subscribe", "insofe_topic_batch43") \
  .option("startingOffsets", "earliest") \
  .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df.printSchema()

df = df.select(col("value").cast("string"), col("timestamp"))
df = df.withColumn('label', split(df.value, "\t")[0])
df = df.withColumn('text', split(df.value, "\t")[1])
df = df.select("label", "text", "timestamp")

df.printSchema()

df = df.withColumn('word', explode(split(col('text'), ' ')))

df = df.select('word','timestamp')

df = df.withWatermark("timestamp", "10 minutes") \
   .groupBy(window(df.timestamp, "10 minutes", "5 minutes"), df.word) \
   .count()

# Start running the query that prints the running counts to the console
#query = df \
#        .writeStream \
#        .format("csv") \
#        .outputMode("append") \
#        .option("truncate","false") \
#        .option("path", "file:///home/jayantm/Kafka/output") \
#        .option("checkpointLocation", "file:///home/jayantm/Kafka/outputCP") \
#        .start()

query = df \
        .writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("truncate", "false") \
        .start()

query.awaitTermination()
