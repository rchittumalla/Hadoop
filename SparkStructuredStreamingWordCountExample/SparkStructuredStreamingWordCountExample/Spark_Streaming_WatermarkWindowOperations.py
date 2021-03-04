
# coding: utf-8

# ### Create SPARK_HOME and PYLIB env var and update PATH env var

import os
import sys
os.environ["SPARK_HOME"] = "/usr/hdp/current/spark2-client"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.4-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")


# ### Initializing Spark

# Build __SparkConf__ object 
# 
#     Contains information about your application.  
# 
# 
# Create __SparkContext__ object 
#     
#     Tells Spark how to access a cluster. 
#     
# 
# Create __SparkSession__ object
# 
#     The entry point to programming Spark with the Dataset and DataFrame API.
# 
#     Used to create DataFrame, register DataFrame as tables and execute SQL over tables etc.

from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

# Import the necessary classes and create a local SparkSession, the starting point of all functionalities related to Spark.
import sys
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window
# from pyspark.sql.types import *
# from pyspark.sql.functions import *

# conf = SparkConf().setAppName("Universal Bank Data Set").setMaster('local')
spark = SparkSession.builder.appName("StructuredNetworkWordCountWatermarkWindowOperation").getOrCreate()
#sc = SparkContext(conf=conf)
#spark = SparkSession(sc)



# Create DataFrame representing the stream of input lines from connection to localhost:10099
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 12075).option('includeTimestamp', 'true').load()
spark.sparkContext.setLogLevel("ERROR")
# Split the lines into words, retaining timestamps
# split() splits each line into an array, and explode() turns the array into multiple rows
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word"),
    lines.timestamp
)

# Generate running word count
# Group the data by window and word and compute the count of each group
windowedCounts = words.withWatermark("timestamp", "3 seconds").groupBy( \
        window(words.timestamp, "10 seconds"),words.word).count()


# Start running the query that prints the running counts to the console
# The query object is a handle to that active streaming query, and we have decided to wait for the termination of the query using awaitTermination() to prevent the process from exiting while the query is active.

# OutputMode - complete/append/update

# Output Modes

# There are a few types of output modes.

#    Append mode (default) - This is the default mode, where only the new rows added to the Result Table since the last trigger will be outputted to the sink. This is supported for only those queries where rows added to the Result Table is never going to change. Hence, this mode guarantees that each row will be output only once (assuming fault-tolerant sink). For example, queries with only select, where, map, flatMap, filter, join, etc. will support Append mode.

#    Complete mode - The whole Result Table will be outputted to the sink after every trigger. This is supported for aggregation queries.

#    Update mode - (Available since Spark 2.1.1) Only the rows in the Result Table that were updated since the last trigger will be outputted to the sink. More information to be added in future releases.


query = windowedCounts.writeStream.outputMode("complete").format("console").option("truncate", 'false').start()

query.awaitTermination()

