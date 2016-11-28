#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
import extra_functions.selection as selection
from extra_functions.filters import *
from extra_functions.constants import *
from extra_functions.helpers import *
from dummy_spark import *
import unittest
import pyspark
import sys
import re

sc = SparkContext()

class SelectionTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_is_emoji(self):
		self.assertTrue(selection.is_emoji(u"\U0001F64F"))
		self.assertFalse(selection.is_emoji(u"hello"))
		with self.assertRaises(TypeError):
			selection.is_emoji(10)


	def test_check_emoji_in_terms(self):
		self.assertTrue(selection.check_emoji_in_terms(self.dummy_sentence_with_emoji.split()))
		self.assertFalse(selection.check_emoji_in_terms(self.dummy_sentence.split()))

	def test_checkTermsInSentence(self):
		list_of_queries = ["lorem", "ipsum"]
		self.assertTrue(selection.checkTermsInSentence(list_of_queries, self.dummy_sentence))
		list_of_queries = ["lorem_ipsum"]
		self.assertTrue(selection.checkTermsInSentence(list_of_queries, self.dummy_sentence))
		list_of_queries = ["lorem ipsum something"]
		self.assertFalse(selection.checkTermsInSentence(list_of_queries, self.dummy_sentence))
		list_of_queries = ["ipsum_lorem"]
		self.assertFalse(selection.checkTermsInSentence(list_of_queries, self.dummy_sentence))
		list_of_queries = ["regex:lore"]
		self.assertTrue(selection.checkTermsInSentence(list_of_queries, self.dummy_sentence))

	def test_checkBagWords(self):
		query = "Lorem ipsum"
		self.assertTrue(selection.checkBagWords(query, self.dummy_sentence.split()))
		query = "Lorem something"
		self.assertFalse(selection.checkBagWords(query, self.dummy_sentence.split()))

	def test_checkMultipleWords(self):
		query = "Lorem_ipsum"
		self.assertTrue(selection.checkMultipleWords(query, self.dummy_sentence))


	def test_checkTermsInLm(self):
		list_of_queries = ["lorem_ipsum"]
		self.assertTrue(selection.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), {}))
		list_of_queries = ["lorem ipsum"]
		self.assertTrue(selection.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), {}))
		list_of_queries = ["lorem_something"]
		self.assertFalse(selection.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), {}))
		list_of_queries = ["lorem something"]
		self.assertFalse(selection.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), {}))		
		list_of_queries = ["lorem","something"]
		self.assertTrue(selection.checkTermsInLm(list_of_queries, self.dummy_sentence.split(), {}))	

	def test_check_query_in_data(self):
		list_of_queries = ["lorem_ipsum"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))
		list_of_queries = ["lorem ipsum"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))
		list_of_queries = ["lorem_something"]
		self.assertFalse(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))
		list_of_queries = ["lorem something"]
		self.assertFalse(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))	
		list_of_queries = ["lorem","something"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))	
		list_of_queries = ["lorem", "ipsum"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence))
		list_of_queries = ["lorem_ipsum"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence))
		list_of_queries = ["lorem ipsum something"]
		self.assertFalse(selection.check_query_in_data(list_of_queries, self.dummy_sentence))
		list_of_queries = ["ipsum_lorem"]
		self.assertFalse(selection.check_query_in_data(list_of_queries, self.dummy_sentence))
		list_of_queries = ["regex:lore"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence))	
		list_of_queries = ["lorem"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence))		
		list_of_queries = ["lorem"]
		self.assertTrue(selection.check_query_in_data(list_of_queries, self.dummy_sentence.split()))		

if __name__ == '__main__':
    unittest.main()


