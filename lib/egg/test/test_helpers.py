#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
import extra_functions.helpers as helpers
from extra_functions.filters import *
from extra_functions.constants import *
from dummy_spark import *
import unittest
import pyspark
import sys
import re

sc = SparkContext()

class HelpersTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_alpha_numeric(self):
		self.assertTrue(alpha_numeric("hello"))
		self.assertFalse(alpha_numeric("^"))

	def test_tokenizer(self):
		self.assertEqual(tokenizer("this is a test"), ["this", "is", "a", "test"])
		self.assertEqual(tokenizer("this is a test?", nltk_tokenizer = True), ["this", "is", "a", "test", "?"])

	def test_group_ngrams(self):
		self.assertEqual(helpers.group_ngrams("this is a test", num_grams = 2), "a_test")
		self.assertEqual(helpers.group_ngrams("this is a test but a bit longer", num_grams = 3), "is_a_test a_test_but test_but_a but_a_bit a_bit_longer")


	def test_check_location(self):
		self.assertTrue(helpers.check_location(0.0, 0.0, ["0.01,0.005", "2.5,8.0"]))
		self.assertFalse(helpers.check_location(20.0, 20.0, ["0.01,0.005", "2.5,8.0"]))

	def test_load_partial_data(self):
		url = "sk-model/jobs/MetadataInferenceRerun2016_OldSDK/"
		self.assertIs(type(helpers.load_partial_data(url, 4, self.sqlContext)), pyspark.rdd.RDD)
		self.assertIs(type(helpers.load_partial_data(url, "4", self.sqlContext)), pyspark.rdd.RDD)

		url = "invalid_url"
		with self.assertRaises(Exception):
			helpers.load_partial_data(url, "4", self.sqlContext)


	def test_date_to_milliseconds(self):
		self.assertEqual(helpers.date_to_milliseconds("01/2014"), 1388534400000.0)
		self.assertEqual(helpers.date_to_milliseconds("01/10/2014"), 1.4121216e+12)
		with self.assertRaises(ValueError):
			helpers.date_to_milliseconds("2014-01-01")

	def test_get_min_max_date(self):
		dummy_list = [{"filters":{"start_date":"01/01/2014"}}, {"filters":{"end_date":"01/01/2015"}}]
		self.assertEqual(helpers.get_min_max_date(dummy_list), (0.0, 1577836800000.0))
		dummy_list = [{"filters":{"start_date":"01/01/2014", "end_date":"01/10/2014"}}, {"filters":{"start_date":"01/01/2013","end_date":"01/01/2015"}}]
		self.assertEqual(helpers.get_min_max_date(dummy_list), (1356998400000.0, 1420070400000.0))

	def test_flatten(self):
		dummy_list = [[[["this"],"this", "is"],"this", "a", "test"], "test"]
		self.assertEqual(list(helpers.flatten(dummy_list)), ["this", "this", "is", "this", "a", "test", "test"])


	def test_cloudUserId_to_uuid_str(self):
		user_id = self.rdd.first().cloudUserId
		self.assertNotEqual(helpers.cloudUserId_to_uuid_str(user_id), "invalid_id")
		user_id = bytes()
		self.assertEqual(helpers.cloudUserId_to_uuid_str(user_id), "invalid_id")

	def test_load_extras(self):
		extras =  [ {"path":"s3a://sk-telemetry-data/app-telemetry/parquet_daily/SwiftKey_Android_prod/1/com.swiftkey.avro.telemetry.core.events.ActivationEvent/date_received=2015-06-08/", 
	 					  			 "format":"parquet", 
	 					  			 "key":"metadata.installId", 
	 					  			 "values":["deviceInfo.os.name"]}]
		self.assertIs(type(helpers.load_extras(extras, sc)), dict)
		self.assertIs(type(helpers.load_extras(extras, sc)[0]), pyspark.rdd.PipelinedRDD)

	def test_extract_sentence_v2(self):
		filters = {"all_dataset":"true"}
		user = self.rdd.first()
		queries = []
		self.assertGreater(helpers.extract_sentence_v2(queries, user, filters), 0)


	def test_process_reconstructed_sentences(self):
		sentences = ["this is a test", "today i had a test", "a trust test for the goverment"]
		filters = {"ngrams":2}
		actual_results = ["a_test", "today_i", "a_test", "a_trust", "trust_test", "test_for", "the_goverment"]
		self.assertEqual(process_reconstructed_sentences(sentences, filters, []), actual_results)



	def test_join_attributes(self):
		attribute_dict = {}
		attribute_dict["gender"] = sc.textFile(PATH2GENDER).map(lambda line: line.split(","))
		self.assertRegexpMatches(helpers.join_attributes(attribute_dict, self.rdd, filtering = False).items()[0][1][0], "Gender_")

	def test_check_if_field_is_not_none(self):
		row = Row(geoIP = Row(country = None, city = Row(name = "Milan", city_id = None)))
		attribute_1 = "geoIP.city.name"
		attribute_2 = "geoIP.city.city_id"
		attribute_3 = "geoIP.country"
		self.assertTrue(helpers.check_if_field_is_not_none(row, attribute_1))
		self.assertFalse(helpers.check_if_field_is_not_none(row, attribute_2))
		self.assertFalse(helpers.check_if_field_is_not_none(row, attribute_3))



if __name__ == '__main__':
    unittest.main()


