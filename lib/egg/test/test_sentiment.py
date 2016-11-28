#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from pyspark import SparkContext, SparkConf, StorageLevel
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
import extra_functions.sentiment as sentiment
#from extra_functions.disambiguation import *
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

class SentimentTest(unittest.TestCase):

	def setUp(self):
		self.word2vecs = sc.broadcast(sc.textFile("s3a://sk-insights/users/francesco/vocabs/glove_vecs_ligth_version.txt")
							 .map(lambda line: line.split())
                             .map(lambda line: convert_line(line))
             				 .filter(lambda line: line is not None)
                             .collectAsMap()
                        	)

		self.sentiment_vector = {}
		self.stopwords = STOPWORDS_1 + WEB_SEARCH + OTHERS
		print self.stopwords
		self.sentiment_vector["POS"] = sentiment.get_merged_vec(POSITIVE_SENTIMENT_DEFAULT_TERMS, self.word2vecs, [], self.stopwords)
		self.sentiment_vector["NEG"] = sentiment.get_merged_vec(NEGATIVE_SENTIMENT_DEFAULT_TERMS, self.word2vecs, [], self.stopwords)

	def test_get_sentiment(self):
		sent1 = "I miss you twitter. My phone broke, now I'm using a stupid Nokia phone. Ughhh"
		label1 = "NEGATIVE"
		sent2 = "i am going out for grocery shopping"
		label2 = "NEUTRAL"
		sent3 = "At work...busy morning already but it's good! Makes the day go faster! "
		label3 = "POSITIVE"
		sent4 = sent3.split()
		print "\n\n\n\n\n"
		print sent1
		print "\n\n\n\n\n"
		self.assertEqual(get_sentiment(sent1, self.sentiment_vector, self.word2vecs, self.stopwords), label1)
		self.assertEqual(get_sentiment(sent2, self.sentiment_vector, self.word2vecs, self.stopwords), label2)
		self.assertEqual(get_sentiment(sent3, self.sentiment_vector, self.word2vecs, self.stopwords), label3)
		self.assertEqual(get_sentiment(sent4, self.sentiment_vector, self.word2vecs, self.stopwords), label3)

	


if __name__ == '__main__':
    unittest.main()


