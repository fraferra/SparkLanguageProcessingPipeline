#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
import extra_functions.formatting as formatting
from pyspark.sql import SQLContext, Row
import extra_functions.helpers as helpers
import extra_functions.filters as filters
from extra_functions.constants import *
from dummy_spark import *
import unittest
import pyspark
import sys
import re

sc = SparkContext()


class FiltersTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_preprocessing_filter_by_terms(self):
		list_of_queries = ["regex:lor"]
		self.assertTrue(filters.preprocessing_filter_by_terms(list_of_queries, self.dummy_sentence.split(), emoji_sentence = True))
		list_of_queries = []
		self.assertTrue(filters.preprocessing_filter_by_terms(list_of_queries, self.dummy_sentence_with_emoji.split(), emoji_sentence = True))
		list_of_queries = ["something", "lorem"]
		self.assertTrue(filters.preprocessing_filter_by_terms(list_of_queries, self.dummy_sentence.split()))

	def test_checkTermsInLm(self):
		list_of_queries = []
		dummy_filters = {"all_dataset":"true", "emoji_sentence":"true"}
		self.assertTrue(filters.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), dummy_filters))
		list_of_queries = []
		self.assertTrue(filters.checkTermsInLm(list_of_queries, self.dummy_sentence_with_emoji.split(), dummy_filters))
		list_of_queries = ["something", "lorem"]
		dummy_filters = {}
		self.assertTrue(filters.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), dummy_filters))


	def test_load_extra_filtering(self):
		extras1 = {"path":"s3a://sk-insights/users/francesco/datasets/gender/MX_GB_IN_gender_data",
				  "format":"csv",
				  "key":1,
				  "values":"Gender_FEMALE",
				  "identifier":0
				}
		results1  = load_extra_filtering(extras1, sc)
		self.assertTrue(all([x == "Gender_FEMALE" for x in results1.values()]))

		extras2 = {"path":"s3a://sk-insights/users/francesco/datasets/gender/MX_GB_IN_gender_data",
				  "format":"csv",
				  "key":1,
				  "values":"Gender_FEMALE"
				}
		self.assertRaises(KeyError, load_extra_filtering(extras2, sc))


	# def test_load_extras_filtering(self):
	# 	extras_list = [ {"path":"s3a://sk-telemetry-data/app-telemetry/parquet_daily/SwiftKey_Android_prod/1/com.swiftkey.avro.telemetry.core.events.ActivationEvent/date_received=2015-12-08/", 
	# 					  		   "format":"parquet", 
	# 					  		   "identifier":"metadata.installId",
	# 					  		   "key":"deviceInfo.os.name", 
	# 					  		   "values":["android"]}
	# 					  		 ]
	# 	l = filters.load_extras_filtering(extras_list, sc)
	# 	self.assertIs(type(l), dict)
	# 	self.assertIs(type(l[0]), pyspark.rdd.PipelinedRDD)

	def test_load_extra_filtering(self):
		extra = {"path":"s3a://sk-telemetry-data/app-telemetry/parquet_daily/SwiftKey_Android_prod/1/com.swiftkey.avro.telemetry.core.events.ActivationEvent/date_received=2015-12-08/", 
						  		   "format":"parquet", 
						  		   "identifier":"metadata.installId",
						  		   "key":"deviceInfo.os.name", 
						  		   "values":["android"]}
		l = filters.load_extra_filtering(extra, sc)
		self.assertIs(type(l), pyspark.rdd.PipelinedRDD)

	def test_filter_query(self):
		row = Row(geoIP = Row(country = "US", city = Row(name = "Seattle")), deviceBrand = "samsung")
		users_list = []
		installId_list = []
		dummy_filters = {	
					"geoIP.country":["US", "GB"]
					}

		self.assertTrue(filters.filter_query(row, dummy_filters, users_list, installId_list))
		dummy_filters = {	
					"geoIP.country":["US", "GB"],
					"deviceBrand":"tcl"
					}

		self.assertFalse(filters.filter_query(row, dummy_filters, users_list, installId_list))





if __name__ == '__main__':
    unittest.main()



    