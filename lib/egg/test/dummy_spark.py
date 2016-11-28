#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SQLContext, Row

def create_sample_rdd(sc):	
	sqlContext = SQLContext(sc)
	path2avro = "s3a://sk-model/jobs/MetadataInferenceRerun2016_OldSDK/part-00000.avro"
	return (sqlContext.read.load(path2avro, "com.databricks.spark.avro").rdd)



def create_sample_parquet(sc):
	sqlContext = SQLContext(sc)
	path2parquet = "s3a://sk-telemetry-data/app-telemetry/parquet_daily/SwiftKey_Android_prod/1/com.swiftkey.avro.telemetry.core.events.ActivationEvent/date_received=2015-12-08/"
	return (sqlContext.read.load(path2parquet).rdd)


def create_sample_text():
	return ("""Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
			   Maecenas lobortis tempus erat, et ornare velit sodales sed. 
			   Curabitur dapibus tempus ante a aliquet.""")

def create_sample_text_with_emoji():
	return (u"""Lorem ipsum dolor sit amet, consectetur 🙁 adipiscing elit. 
			   Maecenas lobortis tempus erat, et ornare velit sodales sed. 
			   Curabitur dapibus tempus ante a 🙁🙁 aliquet.""")