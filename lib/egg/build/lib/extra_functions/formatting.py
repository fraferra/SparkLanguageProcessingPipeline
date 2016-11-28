from dateutil.relativedelta import relativedelta, SU, FR
from pyspark.sql import SQLContext, Row
from nltk.tokenize import word_tokenize
from collections import Counter
from datetime import datetime
from selection import *
from constants import *
from helpers import *
import time
import re

def format_data(data, list_queries, attributes, type_of_analysis, filters, sc, installIdUsed, vec_keywords = None, word2vecs = None, stopwords = [], nltk_tokenizer = False, sentiment_vector = None):
	attribute_dict = {}
	for key in attributes:
		if key == "gender":
			gender_dict = sc.textFile(PATH2GENDER).map(lambda line: line.split(","))
			attribute_dict["gender"] = gender_dict
			#data = data
			data = data.filter(lambda line: check_if_field_is_not_none(line, key, other = True))
		elif key == "extras":
			attribute_dict = dict(attribute_dict.items() + load_extras(attributes["extras"], sc).items())
			data = data.filter(lambda line: check_if_field_is_not_none(line, key, other = True))
		else:
			data = data.filter(lambda line: check_if_field_is_not_none(line, key))

	if attribute_dict != {}:
		attribute_dict_broadcasted = sc.broadcast(join_attributes(attribute_dict, data))
	else:
		attribute_dict_broadcasted = sc.broadcast(attribute_dict)


	if type_of_analysis == "co_occurrence":
		data = (data
				.map(lambda line: (line, Counter(process_reconstructed_sentences(extract_sentence_v2(list_queries, line, filters, stopwords = stopwords, nltk_tokenizer = nltk_tokenizer),
																				 filters, list_queries, stopwords = stopwords, vec_keywords = vec_keywords, word2vecs = word2vecs, nltk_tokenizer = nltk_tokenizer)).items()))
				)
		print "\n\n\n\n"
		print data.take(100)
		print "\n\n\n\n"
		data = (data
				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1]])
			)

	if type_of_analysis == "sentiment":
		data = (data
				.map(lambda line: (line, Counter(process_reconstructed_sentences(extract_sentence_v2(list_queries, line, filters,
																  stopwords = stopwords, 
																  nltk_tokenizer = nltk_tokenizer
																  ), filters,list_queries, stopwords = stopwords, is_sentiment = True, sentiment_vector = sentiment_vector, vec_keywords = vec_keywords, word2vecs = word2vecs, nltk_tokenizer = nltk_tokenizer)).items()))
				)
		print data.take(100)
		data = (data
				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1]])
			)

	if type_of_analysis == "trends":
		data = (data
				.map(lambda line: (line, Counter(process_reconstructed_sentences(
														extract_sentence_v2(list_queries, line, filters, stopwords = stopwords, nltk_tokenizer = nltk_tokenizer),
														filters,list_queries, stopwords = stopwords, vec_keywords = vec_keywords, word2vecs = word2vecs, nltk_tokenizer = nltk_tokenizer, type_of_analysis = type_of_analysis)).items()))
				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1] if checkTermsInLm(list_queries, [word], {}, strict = True)])
			)
	if type_of_analysis == "stats":
		data = (data
			    .map(lambda line: (add_attributes_2("", attributes, line, attribute_dict_broadcasted), (1,  [bytes(line.cloudUserId)])))
			)
	if type_of_analysis == "users_list" or type_of_analysis == "installids_list":
		if type_of_analysis == "users_list":
			data = (data
				    .map(lambda line: (add_attributes_2(cloudUserId_to_uuid_str(line.cloudUserId), attributes, line, attribute_dict_broadcasted)))
				    )
		if type_of_analysis == "installids_list":
			data = (data
				    .map(lambda line: (add_attributes_2(cloudUserId_to_uuid_str(line.installId), attributes, line, attribute_dict_broadcasted)))
				    )			
		data = (data
			    .filter(lambda line: None not in line)
			    #.map(lambda line: line[0])
			    .distinct()
			    .map(lambda line: [line])
				)

	if type_of_analysis != "users_list" and type_of_analysis != "installids_list":
		data = (data.filter(lambda line: None not in line[0]))

	return data


# def format_data(data, list_queries, attributes, type_of_analysis, filters, sc, installIdUsed, vec_keywords = None, word2vecs = None, stopwords = [], nltk_tokenizer = False, sentiment_vector = None):
# 	attribute_dict = {}
# 	for key in attributes:
# 		if key == "gender":
# 			gender_dict = sc.textFile(PATH2GENDER).map(lambda line: line.split(","))
# 			attribute_dict["gender"] = gender_dict
# 			#data = data
# 			data = data.filter(lambda line: check_if_field_is_not_none(line, key, other = True))
# 		elif key == "extras":
# 			attribute_dict = dict(attribute_dict.items() + load_extras(attributes["extras"], sc).items())
# 			data = data.filter(lambda line: check_if_field_is_not_none(line, key, other = True))
# 		else:
# 			data = data.filter(lambda line: check_if_field_is_not_none(line, key))

# 	if attribute_dict != {}:
# 		attribute_dict_broadcasted = sc.broadcast(join_attributes(attribute_dict, data))
# 	else:
# 		attribute_dict_broadcasted = sc.broadcast(attribute_dict)


# 	if type_of_analysis == "co_occurrence":
# 		data = (data
# 				.map(lambda line: (line, Counter(extract_sentence(list_queries, line, filters, vec_keywords = vec_keywords, word2vecs = word2vecs, stopwords = stopwords, nltk_tokenizer = nltk_tokenizer)).items()))
# 				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1]])
# 			)

# 	if type_of_analysis == "sentiment":
# 		data = (data
# 				.map(lambda line: (line, Counter(extract_sentence(list_queries, line, filters, 
# 																  vec_keywords = vec_keywords, 
# 																  word2vecs = word2vecs, 
# 																  stopwords = stopwords, 
# 																  nltk_tokenizer = nltk_tokenizer,
# 																  is_sentiment = True,
# 																  sentiment_vector = sentiment_vector
# 																  )).items()))
# 				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1]])
# 			)

# 	if type_of_analysis == "trends":
# 		data = (data
# 				.map(lambda line: (line, Counter(extract_sentence(list_queries, line, filters, vec_keywords = vec_keywords, word2vecs = word2vecs, stopwords = stopwords, nltk_tokenizer = nltk_tokenizer)).items()))
# 				.flatMap(lambda line: [(add_attributes_2(word, attributes, line[0], attribute_dict_broadcasted), (value, [bytes(line[0].cloudUserId)])) for word, value in line[1] if checkTermsInLm(list_queries, [word], {})])
# 			)
# 	if type_of_analysis == "stats":
# 		data = (data
# 			    .map(lambda line: (add_attributes_2("", attributes, line, attribute_dict_broadcasted), (1,  [bytes(line.cloudUserId)])))
# 			)
# 	if type_of_analysis == "users_list" or type_of_analysis == "installids_list":
# 		if type_of_analysis == "users_list":
# 			data = (data
# 				    .map(lambda line: (add_attributes_2(cloudUserId_to_uuid_str(line.cloudUserId), attributes, line, attribute_dict_broadcasted)))
# 				    )
# 		if type_of_analysis == "installids_list":
# 			data = (data
# 				    .map(lambda line: (add_attributes_2(cloudUserId_to_uuid_str(line.installId), attributes, line, attribute_dict_broadcasted)))
# 				    )			
# 		data = (data
# 			    .filter(lambda line: None not in line)
# 			    #.map(lambda line: line[0])
# 			    .distinct()
# 			    .map(lambda line: [line])
# 				)

# 	if type_of_analysis != "users_list" and type_of_analysis != "installids_list":
# 		data = (data.filter(lambda line: None not in line[0]))

# 	return data
	

def truncate_date(timestamp, time_division):
	timestamp = timestamp/1000
	if time_division == "days":
		return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
	if time_division == "weeks":
		timestamp = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
		timestamp = datetime.strptime(timestamp,'%Y-%m-%d')
		timestamp = timestamp + relativedelta(weekday=SU(-1))
		return timestamp.strftime("%Y-%m-%d")
	if time_division == "months":
		return datetime.fromtimestamp(timestamp).strftime('%Y-%m')
	if time_division == "years":
		return datetime.fromtimestamp(timestamp).strftime('%Y')


def add_attributes_2(key, attributes, line, attribute_dict):
	key = [key.replace("_", " ")]
	for attribute in attributes:
			try:
				if attribute == "gender":
					continue
				elif attribute == "extras":
					continue
				elif attribute == "geoIP.subdivisions":
					key.append(line.geoIP.subdivisions[0].name)
				elif attribute == "enabledLanguages":
					key.append(",".join(line.enabledLanguages))
				elif attribute == "time_division":
					key.append(truncate_date(line.endTimestamp, attributes["time_division"]))
				else:
					if check_if_field_is_not_none(line, attribute):	
						if attribute == "installId":
							key.append(cloudUserId_to_uuid_str(eval("line." + attribute)))
						else:
							key.append(eval("line." + attribute))
					else:
						key.append(None)
			except IndexError:
				key.append(None)
			except AttributeError:
				raise AttributeError(("Wrong attribute name %s")%(attribute))
	if "gender" in attributes or "extras" in attributes:
		key.extend(attribute_dict.value.get(cloudUserId_to_uuid_str(line.cloudUserId), [None]))
	return tuple(key) 

# def add_attributes(key, attributes, line, attribute_dict):
# 	key = [key.replace("_", " ")]
# 	for attribute in attributes:
# 		try:
# 			attribute = attribute.lower()
# 			if attribute == "time_division":
# 				key.append(truncate_date(line.endTimestamp, attributes["time_division"]))
# 			# if attribute == "gender":
# 			# 	key.append(attribute_dict.value["gender"].get(cloudUserId_to_uuid_str(line.cloudUserId), None))
# 			if attribute == "regions":
# 				key.append(line.geoIP.subdivisions[0].name)
# 			if attribute == "country":
# 				key.append(line.geoIP.country) 
# 			if attribute == "brand":
# 				key.append(line.deviceBrand) 
# 			if attribute == "model":
# 				key.append(line.deviceModel) 
# 			if attribute == "city":
# 				key.append(line.geoIP.city.name)
# 			# else:
# 			# 	key.append(attribute_dict.value[attribute].get(cloudUserId_to_uuid_str(line.cloudUserId), None))

# 		except (AttributeError, IndexError) as e:
# 			key.append(None)
# 	if "gender" in attributes or "extras" in attributes:
# 		key.extend(attribute_dict.value.get(cloudUserId_to_uuid_str(line.cloudUserId), [None]))
# 		print key
# 	return tuple(key) 

