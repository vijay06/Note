#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 19:58:18 2019

@author: vijay
"""

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()


kinesisDF = spark \
  .readStream \
  .format("kinesis") \
  .option("streamName", "python-stream") \
  .option("initialPosition", "earliest") \
  .option("region", "us-east-1") \
  .load()
  

query = kinesisDF \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()  

