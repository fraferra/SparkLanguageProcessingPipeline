#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
import extra_functions.load as load
from extra_functions.filters import *
from extra_functions.selection import *
from extra_functions.constants import *
from extra_functions.helpers import *
from boto.exception import *
from dummy_spark import *
import unittest
import pyspark
import sys
import re

sc = SparkContext()

class LoadTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_format_date(self):
		date1 = "03/01/2014"
		result1 = "2014-01-03"
		self.assertEqual(load.format_date(date1), result1)

	def test_get_starting_date(self):
		s1 = "path/to/something/date_received=2015-09-13"
		result1 = "2015-09-13"
		self.assertEqual(load.get_starting_date(s1), result1)

	def test_get_fraction(self):
		original_rdd = sc.parallelize(range(1000))
		sampling_fraction = 0.2
		filtered_rdd = load.get_fraction(original_rdd, sampling_fraction)
		self.assertLess(filtered_rdd.count(), 250)

	def test_list_files(self):
		bucket = "sk-insights"
		path = "pipeline/augmenter/0"
		len_result = len(load.list_files(bucket, path))
		self.assertGreater(len_result, 0)
		bucket = "sk-not-existent"
		#len_result = len(load.list_files(bucket, path))
		with self.assertRaises(Exception):
			load.list_files(bucket, path)

	def test_determine_interval(self):
		start1 = 20130101
		end1 = 20140101
		result1 = "old"
		self.assertEqual(load.determine_interval(start1, end1), result1)
		start2 = 20130101
		end2 = 20170101
		result2 = "old_and_new"
		self.assertEqual(load.determine_interval(start2, end2), result2)
		start3 = 20160601
		end3 = 20170101
		result3 = "new"
		self.assertEqual(load.determine_interval(start3, end3), result3)





if __name__ == '__main__':
    unittest.main()


