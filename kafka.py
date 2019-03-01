from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql import Row
import boto3
import re
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *


#session = boto3.Session(profile_name="dynamodb_s3")
#log_table = session.resource('dynamodb',region_name='us-east-1').Table('Log_store')


#log_table.put_item(Item=data)
    


spark = SparkSession \
    .builder \
    .appName("kafka_log_process") \
    .getOrCreate()

Schema = StructType([ StructField("index", StringType(), True), StructField("host", StringType(), True),StructField("time", StringType(), True),StructField("request", StringType(), True),StructField("status", StringType(), True),StructField("size", StringType(), True) ])


df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "log") \
  .option("startingOffsets", "earliest") \
  .load().select(from_json(col("value").cast("string"),Schema).alias("parsed_value"))

df = df.select("parsed_value.*")
df.printSchema()

df.writeStream.format("console").option("truncate","false").start().awaitTermination()


