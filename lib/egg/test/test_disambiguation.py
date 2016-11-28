#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
import extra_functions.disambiguation as disambiguation
from extra_functions.filters import *
from extra_functions.selection import *
from extra_functions.constants import *
from extra_functions.helpers import *
from dummy_spark import *
import unittest
import pyspark
import sys
import re

sc = SparkContext()

class DisambiguationTest(unittest.TestCase):

	def setUp(self):
		self.rdd = create_sample_rdd(sc)
		self.parquet = create_sample_parquet(sc)
		self.dummy_sentence = create_sample_text()
		self.dummy_sentence_with_emoji = create_sample_text_with_emoji()
		self.sqlContext = SQLContext(sc)

	def test_is_same_context(self):
		word2vecs = sc.broadcast(sc.textFile("s3a://sk-insights/users/francesco/vocabs/glove_vecs_ligth_version.txt")
							 .map(lambda line: line.split())
                             .map(lambda line: disambiguation.convert_line(line))
             				 .filter(lambda line: line is not None)
                             .collectAsMap()
                        	)
		words = "have you watched the last of film of ford".split()
		query = ["ford"]
		keywords = ["movie", "oscars"]
		stopwords = STOPWORDS_1 + WEB_SEARCH + OTHERS
		vec_keywords = disambiguation.get_merged_vec(words, word2vecs, query, stopwords)
		self.assertTrue(is_same_context(words, vec_keywords, query, stopwords, word2vecs))
		words = "i have just bought a ford".split()
		self.assertFalse(is_same_context(words, vec_keywords, query, stopwords, word2vecs))


	def test_is_alphanumeric(self):
		word1 = "ciao"
		self.assertTrue(alpha_numeric(word1))
		word2 = "2015"
		self.assertFalse(alpha_numeric(word2))
		word3 = ":)"
		self.assertTrue(alpha_numeric(word3))		


if __name__ == '__main__':
    unittest.main()


