#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
import extra_functions.formatting as formatting
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


class FormattingTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_truncate_date(self):
		timestamp =  helpers.date_to_milliseconds("01/10/2014")
		self.assertEqual(formatting.truncate_date(timestamp, "years"), "2014")
		self.assertEqual(formatting.truncate_date(timestamp, "months"), "2014-10")
		self.assertEqual(formatting.truncate_date(timestamp, "days"), "2014-10-01")
		self.assertEqual(formatting.truncate_date(timestamp, "weeks"), "2014-09-28")

	def test_add_attributes_2(self):
		row = Row(deviceBrand = "iphone", deviceModel = "iphone_6")
		attributes = {"deviceBrand":"true", "deviceModel":"true"}
		key = "some_writing"
		self.assertItemsEqual(formatting.add_attributes_2(key, attributes, row, {}), ("some writing", "iphone", "iphone_6"))






if __name__ == '__main__':
    unittest.main()
