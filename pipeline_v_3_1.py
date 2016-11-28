from pyspark import SparkContext, SparkConf, StorageLevel
from py4j.protocol import Py4JError,Py4JJavaError
from extra_functions.disambiguation import *
from extra_functions.formatting import *
from pyspark.sql import SQLContext, Row
from extra_functions.helpers import *
from extra_functions.filters import *
from extra_functions.mail import *
from extra_functions.load import *
import extra_functions.constants
from collections import Counter
import itertools
import argparse
import codecs
import smtplib
import json
import sys
import re
import os


class Pipeline:

	def __init__(self, params, sc, sqlContext):
		self.sqlContext = sqlContext
		self.sc = sc
		self.params = self.initialize_params(params)
		self.all_data = self.pre_process_data(self.load_data())
		#print self.all_data.take(100)
		self.email_object = self.load_email_object()

	
	def load_email_object(self):
		"""
		Create email object is email addresses are provided
		"""
		if self.params["output_email"] is not None:
			return Email(self.params["output_email"].split(","), "Results From Pipeline", self.sc)
		else:
			return None
	
	def initialize_params(self, params):
		"""
		Initialize all the parameters needed for pipeline
		and for pre-processing (caching the initial data for all the queries)
			
		"""
		finalParams = {
			"all_query_terms" :[],
			"all_dataset":False,
			"queries" :[],
			"min_date" :False,
			"max_date" : False,
			"num_fragments" : False,
			"bad_data":True,
			"input_folder" : "",
			"input_type" : "avro",
			"output_folder" : "",
			"emoji_sentence":False,
			"output_email": None,
			"nltk_tokenizer":False,
			"stopwords": []
			}

		try:
			# Input_folder - where pipeline looks for data
			finalParams["input_folder"] = params.get("input_folder", PATH2OLDDATA)
			# Output folder - where to save it
			finalParams["output_folder"] = params["output_folder"]
			# Gather all keywords from all the queries in order to pre-filter the whole avro dataset
			finalParams["all_query_terms"] = list(itertools.chain(*[d.get("query", []) for d in params["queries"]]))
			# If one of the query needs the whole dataset (example: stats for the whole population) then this parameter
			# is true and the data is not pre-filtered
			finalParams["all_dataset"] = True in [True if "all_dataset" in q.get("filters", {}) else False for q in params["queries"]]
			# If one of the queries has emoji_sentence set to true then the dataset will be pre-processed and all the
			# fragments containing emojis will be cached
			finalParams["emoji_sentence"] = True in [True if "emoji_sentence" in q.get("filters", {}) else False for q in params["queries"]]
			# List of queries
			finalParams["queries"] = params["queries"]
			finalParams["bad_data"] = "true" == params.get("bad_data", "true") 
			# Total number of avro partitions to process. If not present the whole avro dataset will be processed
			finalParams["num_fragments"] = params.get("num_fragments", False)
			# use nltk tokenizer instead of nomal splitting. more time consuming
			finalParams["nltk_tokenizer"] = params.get("nltk_tokenizer", "false") == "true"
			# output email used to email results
			finalParams["output_email"] = params.get("output_email", None)
			# type of input dataset - so far supported only for avro
			finalParams["input_type"] = params.get("input_type", "avro")
			# path to file containing additional stopwords 
			finalParams["stopwords"] =  STOPWORDS_1 + WEB_SEARCH + OTHERS + self.load_additional_stopwords(params.get("stopwords", None))

			#self.stopwords += finalParams["stopwords"]
			# min_date and max date oin the queries. the are used to further reduce the cached dataset
			finalParams["min_date"], finalParams["max_date"] = get_min_max_date(params["queries"])

			print finalParams
		except KeyError:
			raise KeyError("Missing JSON paramenter missing in conf file")
		
		return finalParams

	def load_additional_stopwords(self, path):
		"""
		If no path is provided the stopwords will be filtered based on the ones stored in constants.py
		If provided the file should be a text file containing words in a single column
		return - list of stopwords
		"""
		if path is None:
			return []
		else:
			return (self.sc.textFile(path).collect())

	def process_all_queries(self):
		"""
		It loops through each individual query and processes it
		"""
		for query in self.params["queries"]:
			try:
				self.process_query(query)
			except EOFError:
				pass
	

	def load_data(self):
		"""
		Load the avro file and it returns an RDD
		"""

		bucket = NEW_DATA_BUCKET

		old_path = self.params["input_folder"]

		new_path = PAH2NEWDATA

		data = join_old_new_data_2(bucket, old_path, new_path, self.sqlContext, self.sc,
                      sampling_fraction = self.params["num_fragments"], 
                      start_date = self.params["min_date"], 
                      end_date = self.params["max_date"])

		print "\n\n\n\n\n\n"
		#print data.take(100)
		print "\n\n\n\n\n\n"

		return data


	def pre_process_data(self, data):
		"""
		Pre-processing phase before analyzing single queries. It chaches the dataset to imporve perfomance based on
		the aggregate keywords from different queries and based on the dates limits used
		"""
		return pre_processing_filter(data, self.params["all_query_terms"], self.params["min_date"], self.params["max_date"], self.params["all_dataset"], self.params["emoji_sentence"], bad_data = self.params["bad_data"]).cache()


	def process_query(self, query):
		"""
		It processes the query based on the type of analysis. In the first stage it filters based on certain fields
		(example: geoIP.country = US). In the second stage it formatts the query (example: it adds gender to each word or device model).
		The third stage consists in a second filter that removes stopwords and thresholds the results and then it saves the
		results.
		"""
		try:
			ngrams = query.get("ngrams", 1)
			type_analysis = query["type_of_analysis"]
			list_queries = query.get("query", [])
			filters = query.get("filters", {})
			attributes = query.get("attributes", {})
			disambiguation_keywords = query.get("disambiguation_keywords", [])
			# InstallIdUsed is used in case installId is a key of the extra attributes
			installIdUsed = True if "extras" in attributes and True in ["installId" in str(q.get("key", "")) for q in attributes.get("extras", [])] else False

		except KeyError:
			raise KeyError("\n\nMissing Query parameter: type_of_analysis is needed\n\n")


		if disambiguation_keywords != []:
			word2vecs = sc.broadcast(self.sc.textFile(PATH2WORDVECS)
							 .map(lambda line: line.split())
             				 .map(lambda line:convert_line(line))
             				 .filter(lambda line: line is not None)
             				 .collectAsMap()
                        	)
			vec_keywords = get_merged_vec(disambiguation_keywords, word2vecs, list_queries, self.params["stopwords"])
		else:
			word2vecs = None
			vec_keywords = None


		if filters != {}:		
			data = first_stage_filtering(self.all_data, list_queries, filters, self.sc)
			if "extras" in filters:
				data = filter_external_datasets(data, filters["extras"], self.sc)
		else:
			data = self.all_data


		if (type_analysis == "co_occurrence" 
			or type_analysis == "stats" 
			or type_analysis == "trends"):
			data = self.process_co_occurrence_stats_trends(data, list_queries, filters, attributes, type_analysis, installIdUsed, 
														   vec_keywords = vec_keywords, word2vecs = word2vecs)
		
			if type_analysis != "stats":
				data = second_stage_filtering(data, filters, self.params["stopwords"], nltk_tokenizer = self.params["nltk_tokenizer"])

			data = (data
					.sortBy(lambda line: -line[2])
					#.map(lambda line: list(line[0])  + [line[1], line[2]])
					.map(lambda line: (line[0], line[1], line[2]))
					)

		if type_analysis == "users_list" or type_analysis == "installids_list":
			data = self.process_users_list(data, list_queries, filters, attributes, type_analysis, installIdUsed)

		if type_analysis == "sentiment":
			data = self.process_sentiment(data, list_queries, filters, attributes, type_analysis, installIdUsed)			

		self.save_data(data, query)


	def process_co_occurrence_stats_trends(self, data, list_queries, filters, attributes, type_analysis, installIdUsed, vec_keywords = None, word2vecs = None):
		"""
		Used to format and reduce by key co_occurrence, stats and trends
		"""
		data = format_data(data, list_queries, attributes, type_analysis, filters, self.sc, installIdUsed, 
								 vec_keywords = vec_keywords, 
								 word2vecs = word2vecs, 
								 stopwords = self.params["stopwords"], 
								 nltk_tokenizer = self.params["nltk_tokenizer"])
		data = (data
					.reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))
					.map(lambda line: (line[0], line[1][0], len(set(line[1][1]))))

			)
		return data

	def process_sentiment(self, data, list_queries, filters, attributes, type_analysis, installIdUsed, vec_keywords = None, word2vecs = None):
		"""
		Used to format and reduce sentiment
		"""
		word2vecs = sc.broadcast(self.sc.textFile(PATH2WORDVECS)
							 .map(lambda line: line.split())
             				 .map(lambda line:convert_line(line))
             				 .filter(lambda line: line is not None)
             				 .collectAsMap()
                        	)

		sentiment_vector = {}
		sentiment_vector["POS"] = get_merged_vec(POSITIVE_SENTIMENT_DEFAULT_TERMS, word2vecs, [], self.params["stopwords"])
		sentiment_vector["NEG"] = get_merged_vec(NEGATIVE_SENTIMENT_DEFAULT_TERMS, word2vecs, [], self.params["stopwords"])

		data = format_data(data, list_queries, attributes, type_analysis, filters, self.sc, installIdUsed, 
								 vec_keywords = vec_keywords, 
								 word2vecs = word2vecs, 
								 stopwords = self.params["stopwords"], 
								 nltk_tokenizer = self.params["nltk_tokenizer"],
								 sentiment_vector = sentiment_vector
								 )
		data = (data
					.reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1]))
					.map(lambda line: (line[0], line[1][0], len(set(line[1][1]))))
					.sortBy(lambda line: -line[2])
					#.map(lambda line: list(line[0])  + [line[1], line[2]])
					.map(lambda line: (line[0], line[1], line[2]))
			)
		return data

	def process_users_list(self, data, list_queries, filters, attributes, type_analysis, installIdUsed):
		"""
		Used to format the list of users
		"""
		data = format_data(data, list_queries, attributes, type_analysis, filters, self.sc, installIdUsed)
		return data


	def save_data(self, data, query):
		"""
		Saves the data after processing a query. It sends also an email in case an output email was passed at the beginning
		"""
		filename = "s3a://" + self.params["output_folder"] + query["type_of_analysis"] + "/" + query["name"]

		data = (data
				#.map(lambda line: u"\t".join([x.decode("utf-8") for x in line][0]) + u"\t" + u"\t".join([unicode(x) for x in line[1:]]))
				.map(lambda line: u"\t".join(line[0]) + u"\t" + u"\t".join([str(x) for x in line[1:]]))
				.map(lambda line: line.strip())

				#.map(lambda line: u"\t".join([x.decode("utf-8") for x in line]))

				.coalesce(1)
				)

		trial = 1
		ext = "_" + str(trial)
		flag = False

		# this way files can get saved even if there is a file with the same name already
		while flag is False:
			if trial > 15:
				flag = True
				break
			try:
				(data.saveAsTextFile(filename + ext))
				if self.email_object is not None:
					try:
						self.email_object.send_mail("frferrar@microsoft.com", self.params["output_email"].split(","), filename + ext, query)
					except smtplib.SMTPAuthenticationError:
						sys.stderr.write("\n\n\nADD CREDENTIALS TO spark-env.sh\n\n\n")

				flag = True
				break
			except Py4JJavaError as e:
				trial += 1
				ext = "_" + str(trial)




if __name__ == "__main__":

	sc = SparkContext()
	sqlContext = SQLContext(sc)


	parser = argparse.ArgumentParser(description = 'Pipeline 3.0')
	parser.add_argument("--file", help = "path to JSON conf file", type = str, default = None)

	args = parser.parse_args() 

	if args.file != None:
		try:
			with open(args.file) as data_file:
				params = json.load(data_file)
		except ValueError:
			raise ValueError("\nJSON file not properly formatted.\n")

	pipe = Pipeline(params, sc, sqlContext)
	
	pipe.process_all_queries()
	
