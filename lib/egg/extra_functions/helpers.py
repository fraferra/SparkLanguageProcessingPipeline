from dateutil.relativedelta import relativedelta, SU, FR
from nltk.tokenize import word_tokenize
from pyspark.sql import SQLContext, Row
from collections import Counter
from pidgin import PidginModel
from datetime import datetime
from disambiguation import *
from random import shuffle
from constants import *
from sentiment import *
from selection import *
import ghostrecon
import subprocess
import uuid
import time
import sys
import re

def tokenizer(sentence, nltk_tokenizer = False):
	if nltk_tokenizer:
		return word_tokenize(sentence)
	else:
		return sentence.split()


def check_if_field_is_not_none(line, field, other = False):
	if other or field in ["time_division", "gender", "extras"]:
		return True
	elif field == "geoIP.subdivisions":
		if (line.geoIP is not None 
			and line.geoIP.subdivisions is not None
			and len(line.geoIP.subdivisions)>0
			and line.geoIP.subdivisions[0].name is not None):
			return True
		else:
			return False	
	elif field == "installId":
		if cloudUserId_to_uuid_str(line.installId) != "invalid_id":
			return True
		else:
			return False
	else:
		fields = field.split('.')
		for i in range(1,len(fields)+1):
			if eval("line." + '.'.join(fields[:i])) is None:
				return False
		return True

def group_ngrams(sent, num_grams = 1, nltk_tokenizer=False, type_of_analysis = None, queries = []):
	try:
		if type_of_analysis == None:
			if num_grams == "all":
				return sent.replace(" ", "_")
			else:	
				return (" ".join(["_".join(tokenizer(sent, nltk_tokenizer=nltk_tokenizer)[i:i+num_grams]) for i in range(len(tokenizer(sent, nltk_tokenizer=nltk_tokenizer))) if len(tokenizer(sent, nltk_tokenizer=nltk_tokenizer)[i:i+num_grams]) == num_grams and not set(tokenizer(sent,nltk_tokenizer=nltk_tokenizer)[i:i+num_grams]).issubset(set(STOPWORDS_1 + OTHERS + WEB_SEARCH))])).strip()
		else:
			return reformat_trend(sent, queries)
	except TypeError:
		sys.stderr.write("Invalid Type\n")

def reformat_trend(s,k):
    for q in k:
        s = s.replace(q.replace("_"," "), q)
    return s

def check_location(geoIPLatitude, geoIPLongitude, locations):
	try:
		delta_latitude = 1.6
		delta_longitude = 1.6
		radius = 0.5 # 2.0 latitude ~= 220km
		for location in locations:
			latitude = float(location.split(',')[0])
			longitude = float(location.split(',')[1])
			if (geoIPLatitude - latitude)**2 + (geoIPLongitude - longitude)**2 <= radius**2:
				return True
		return False
	except TypeError:
		raise TypeError("Invalid Type, enter string for location\n")

def load_partial_data(path, size, sqlContext):
	try:
	    ls_lines = ["s3a://" + path + line.split()[-1] for line in subprocess.check_output(['aws','s3','ls',path]).splitlines() if "avro" in line.split()[-1]]
	    shuffle(ls_lines)
	    try:
	    	ls_lines = ls_lines[:size]
	    except TypeError:
	    	sys.stderr.write("size must be an integer\n")
	    	size = int(size)
	    	ls_lines = ls_lines[:size]
	    data = sqlContext.read.load(ls_lines[0], "com.databricks.spark.avro")
	    for l in ls_lines[1:]:
	        data = data.unionAll(sqlContext.read.load(l, "com.databricks.spark.avro"))
	    return data.rdd
	except subprocess.CalledProcessError:
	 	raise Exception("Check if you have AWS permissions to read bucket or if you entered correct path.\n")



def get_min_max_date(queries):
	try:
		starting_dates = [q["filters"].get("start_date","01/01/1970") for q in queries if "filters" in q]
		end_dates = [q["filters"].get("end_date","01/01/2020") for q in queries if "filters" in q]
		min_date = min([date_to_milliseconds(x) for x in starting_dates])	
		max_date = max([date_to_milliseconds(x) for x in end_dates])
		return (min_date, max_date)
	except ValueError:
		sys.stderr.write("Date must in format dd/mm/yyyy\n")


def date_to_milliseconds(date):
	try:
		len_date = len(date.split("/"))
		if len_date == 3:
			timestamp = time.mktime(datetime.strptime(date, "%d/%m/%Y").timetuple())
		if len_date == 2:
			timestamp = time.mktime(datetime.strptime(date, "%m/%Y").timetuple())
		if len_date == 1:
			timestamp = time.mktime(datetime.strptime(date, "%Y").timetuple())
		return timestamp*1000
	except ValueError:
		raise ValueError("Wrong Date Format. Enter dd/mm/yyyy")



# def extract_sentence(query, data, filters, vec_keywords = None, word2vecs = None, stopwords = [], nltk_tokenizer = False):
# 	tmp_reconstruction=''
# 	ngrams = []
# 	lm = PidginModel(bytes(data.pidginData))
# 	recon_term_count=0
# 	recon_sequences=0
# 	sentence=''
# 	emoji_sentence = filters.get("emoji_sentence", "false") == "true"
# 	all_sentence = filters.get("all_dataset", "false") == "true"
# 	num_grams = filters.get("ngrams", 1)
# 	sys.stderr.write("RECON\n\n")
# 	print "RECON\n\n"
# 	for reconstruction in ghostrecon.reconstruct(lm, max_order=4):
# 		if reconstruction.incomplete_start: continue
# 		if reconstruction.incomplete: continue
# 		if reconstruction.incomplete_end: continue
# 		recon_term_count += len(reconstruction)
# 		recon_sequences += 1
# 		sentence=' '.join(reconstruction).lower()
# 		#if check_query_in_data(query, sentence) and tmp_reconstruction.lower() in sentence.lower() and len(ngrams)>0:
# 		# if checkTermsInSentence(query, sentence) and tmp_reconstruction.lower() in sentence.lower() and len(ngrams)>0:
# 		# 	ngrams.pop()
# 		# 	tmp_reconstruction = ''
			
# 		if all_sentence:
# 			ngrams.extend(group_ngrams(sentence, num_grams = num_grams, nltk_tokenizer=nltk_tokenizer).lower().split())

# 		elif emoji_sentence:
# 			if check_emoji_in_terms(tokenizer(sentence.lower(), nltk_tokenizer = nltk_tokenizer)):
# 				ngrams.extend(group_ngrams(sentence, num_grams = num_grams).lower().split())
# 				tmp_reconstruction = sentence.lower()	
# 		else:
# 			#if check_query_in_data(query, sentence):
# 			if checkTermsInSentence(query, sentence):

# 				sys.stderr.write(sentence + "\n\n")
# 				print sentence + "\n\n"
# 				if vec_keywords is None:
# 					ngrams.extend(group_ngrams(sentence, num_grams = num_grams, nltk_tokenizer=nltk_tokenizer).lower().split())
# 					tmp_reconstruction=sentence.lower()
# 				else:
# 					same_context = is_same_context(tokenizer(sentence, nltk_tokenizer = nltk_tokenizer), vec_keywords, query, stopwords, word2vecs)
# 					# sys.stderr.write(str(same_context) + "\n")
# 					# sys.stderr.write(sentence + "\n")
# 					if same_context:
# 						ngrams.extend(group_ngrams(sentence, num_grams = num_grams, nltk_tokenizer = nltk_tokenizer).lower().split())
# 						tmp_reconstruction=sentence.lower()
# 	return ngrams


def extract_sentence_v2(query, data, filters, stopwords = [], nltk_tokenizer = False):
	tmp_reconstruction=''
	ngrams = []
	lm = PidginModel(bytes(data.pidginData))
	recon_term_count=0
	recon_sequences=0
	sentence=''
	emoji_sentence = filters.get("emoji_sentence", "false") == "true"
	all_sentence = filters.get("all_dataset", "false") == "true"
	num_grams = filters.get("ngrams", 1)
	#sys.stderr.write("RECON\n\n")
	print "RECON\n\n"
	for reconstruction in ghostrecon.reconstruct(lm, max_order=4, exclude_initial_terms=[]):
		if reconstruction.incomplete_start: continue
		if reconstruction.incomplete: continue
		if reconstruction.incomplete_end: continue
		recon_term_count += len(reconstruction)
		recon_sequences += 1
		sentence=' '.join(reconstruction).lower()
		#if check_query_in_data(query, sentence) and tmp_reconstruction.lower() in sentence.lower() and len(ngrams)>0:
		if checkTermsInSentence(query, sentence) and tmp_reconstruction.lower() in sentence.lower() and len(ngrams)>0:
			ngrams.pop()
			tmp_reconstruction = ''
			
		if all_sentence:
			ngrams.append(sentence)

		elif emoji_sentence:
			if check_emoji_in_terms(tokenizer(sentence.lower(), nltk_tokenizer = nltk_tokenizer)):
				ngrams.append(sentence)
				tmp_reconstruction = sentence.lower()	
		else:
			#if check_query_in_data(query, sentence):
			if checkTermsInSentence(query, sentence):
				#sys.stderr.write(sentence + u"\n\n")
				ngrams.append(sentence)
				tmp_reconstruction=sentence.lower()
	#sys.stderr.write("NGRAMS\n\n")
	#sys.stderr.write(str(ngrams))
	#sys.stderr.write("END\n\n")
	return ngrams


def process_reconstructed_sentences(sentences, filters, query, num_grams = 1, is_sentiment = False, 
									vec_keywords = None, word2vecs = None, nltk_tokenizer = False, 
									sentiment_vector = None, stopwords = [], type_of_analysis = None):
	ngrams  = []
	num_grams = filters.get("ngrams", 1)

	# max_query_ngram = max([len(x.replace("_", " ").split()) for x in query])


	# if type_of_analysis == "trends" and max_query_ngram > num_grams:
	# 	num_grams = max_query_ngram

	for sentence in sentences:
		if is_sentiment:
			sentiment = get_sentiment(tokenizer(sentence, nltk_tokenizer = nltk_tokenizer), sentiment_vector, word2vecs, stopwords, word = query)
			matching_query = get_matching_keyword(sentence, query)
			ngrams.append(matching_query + "\t" + sentiment)
		else:
			if vec_keywords is None:
				ngrams.extend(group_ngrams(sentence, num_grams = num_grams, nltk_tokenizer=nltk_tokenizer, type_of_analysis=type_of_analysis, queries=query).lower().split())
				sys.stderr.write("FORMATTED NGRAMS\n\n")
				sys.stderr.write(str(ngrams))
				sys.stderr.write("END\n\n")
			else:
				same_context = is_same_context(tokenizer(sentence, nltk_tokenizer = nltk_tokenizer), vec_keywords, query, stopwords, word2vecs)
				if same_context:
					ngrams.extend(group_ngrams(sentence, num_grams = num_grams, nltk_tokenizer = nltk_tokenizer, type_of_analysis=type_of_analysis, queries=query).lower().split())			
	return ngrams

def get_matching_keyword(sentence, queries):
	sentence = sentence.lower()
	for q in queries:
		q  = q.lower().replace("_", " ")
		if q in sentence:
			return q
	return "query_not_found"

def alpha_numeric(word):
	return re.match('^[\w-]+$', word) is not None

def flatten(container):
    for i in container:
        if isinstance(i, (list,tuple)):
            for j in flatten(i):
                    yield j
        else:
            yield i

def join_attributes(attribute_dict, data, filtering = False):
	"""
		Join users and attributes in order to create a user->attributes map
		When it is not for filtering it checks also if the number of attributes is correct
	"""
	users = (data
				.map(lambda line: cloudUserId_to_uuid_str(line.cloudUserId))
				.map(lambda line: (line, "placeholder"))
			)

	num_attributes = len(attribute_dict)

	for attribute, dict_attribute in attribute_dict.items():
		users = users.join(dict_attribute)

	users = (users
			.map(lambda line: (line[0], [x for x in list(flatten(line[1])) if x != "placeholder"]))
			)
	if filtering == False:
		users = (users
			.filter(lambda line: len(line[1]) == num_attributes)
			)
	users = (users.collectAsMap())

	return users

def load_extras(extras_list, sc):
	"""
		Load extra attributes that are not included in the language data
	"""
	extras = {}
	c = 0
	for extra in extras_list:
		try:
			if extra["format"] == "csv":
				key = extra["key"]
				values = extra["values"]
				extras[c] = (sc.textFile(extra["path"])
							.map(lambda line: line.split(","))
							.map(lambda line: (line[key], [line[v] for v in values]))
							)
			if extra["format"] == "parquet":
				key = extra["key"]
				values = extra["values"]
				sqlContext = SQLContext(sc)
				if "installId" in key:
					installId_cloudUserId = (sc.textFile(PATH2INSTALLIDS).map(lambda line: line.split(","))
											   .map(lambda line: (line[0], line[1])))


					extras[c] = (sqlContext.read.load(extra["path"])
						         .rdd
								 .map(lambda line: (eval("line." + key), [eval("line." + v) for v in values]) )
								 .filter(lambda line: line[0] is not None and None not in line[1])
								 .map(lambda line: (cloudUserId_to_uuid_str(line[0]), line[1]))
								 .join(installId_cloudUserId)
								 .map(lambda line: line[1])
								 .map(lambda line: (line[-1], line[0]))
								 )

				else:
					extras[c] = (sqlContext.read.load(extra["path"])
						         .rdd
								 .map(lambda line: (eval("line." + key), [eval("line." + v) for v in values]) )
								 .filter(lambda line: line[0] is not None and None not in line[1])
								 .map(lambda line: (cloudUserId_to_uuid_str(line[0]), line[1]))
								 )
		except KeyError:
			raise KeyError("Missing key for extra attribute\n")
		c += 1
	return extras



def cloudUserId_to_uuid_str(cloudUserId):
    """
    Converts the given cloudUserId in bytearray format to an uuid string.
    param: cloudUserId - a bytearray in little endian format.
    return: uuid_str - a string representing the uuid for the given bytearray.
    """
    cloudUserId = bytes(cloudUserId)
    if len(cloudUserId) == 16:
        return str(uuid.UUID(bytes=cloudUserId[::-1]))
    else:
        return 'invalid_id'


