from dateutil.relativedelta import relativedelta, SU, FR
from nltk.tokenize import word_tokenize
from collections import Counter
from datetime import datetime
from pidgin import PidginModel
from selection import *
from constants import *
from helpers import *
from formatting import *
import time
import re

def pre_processing_filter(data, list_of_queries, start_date, end_date, all_dataset, emoji_sentence, bad_data = True):
	if bad_data:
		data = filter_bad_data(data)
	data = (data
		    .filter(lambda line: line.endTimestamp >= start_date)
		    .filter(lambda line: line.endTimestamp <= end_date)
		    )
	if all_dataset == False:
		data = (data
		    	.filter(lambda line: preprocessing_filter_by_terms(list_of_queries, PidginModel(bytes(line.pidginData)).terms, emoji_sentence = emoji_sentence))
		    	)
	return data


def filter_bad_data(data):
    return (data
			 .filter(lambda line: line.containsPersonalizationData == False)
			 .filter(lambda line: line.implausibleUseCountForTimePeriod == False)
			 .filter(lambda line: line.possibleRetry == False)
			 .filter(lambda line: line.possibleEntireDynamicModel == False)
			 .filter(lambda line: line.possiblePersonalizationData == False)
			 .filter(lambda line: line.possibleCorruption == False)
			)	

def preprocessing_filter_by_terms(list_of_queries, terms, emoji_sentence = False):
	if emoji_sentence:
		if check_emoji_in_terms(terms):
			return True		
	flag = False 
	terms = [x.lower() for x in terms]
	for q in list_of_queries:
		q = q.lower()	
		if "regex" in q:
			q = q.replace('regex:','').replace('_',' ')
			if not all(item == None for item in [re.match(q,term) for term in terms]):
				flag = True
				break
		else:
			if set(q.split()).issubset(set(terms)):
				flag=True
				break
			if set(q.split('_')).issubset(set(terms)):
				flag=True
				break
	return flag


# def filter_external_datasets(data, extras, sc):
# 	users_dict = sc.broadcast(join_attributes(load_extras_filtering(extras, sc), data, filtering = True))

# 	data = (data
# 			.filter(lambda line: users_dict.value.get(cloudUserId_to_uuid_str(line.cloudUserId), False) != False)
# 			)

# 	return data

def filter_external_datasets(data, extras, sc):
	for extra in extras:
		users_dict =  sc.broadcast(load_extra_filtering(extra, sc).collectAsMap())
		if "installId" in extra["identifier"]:
			data = (data
					.filter(lambda line: users_dict.value.get(cloudUserId_to_uuid_str(line.installId), False) != False)
					)	
		else:		
			data = (data
					.filter(lambda line: users_dict.value.get(cloudUserId_to_uuid_str(line.cloudUserId), False) != False)
					)	
	return data

def load_extra_filtering(extra, sc):
	try:
		result = {}
		if extra["format"] == "csv":
			key = extra["key"]
			identifier = extra["identifier"]
			values = extra["values"]

			result = (sc.textFile(extra["path"])
						.map(lambda line: line.split(","))
						.filter(lambda line: str(line[key]).lower() in [str(x).lower() for x in values])
						.map(lambda line: (line[identifier], True))
						)

		if extra["format"] == "parquet":
			key = extra["key"]
			values = extra["values"]
			identifier = extra["identifier"]
			sqlContext = SQLContext(sc)
			result = (sqlContext.read.load(extra["path"])
				         .rdd
				         .filter(lambda line: eval("line." + key) is not None )
						 .filter(lambda line: str(eval("line." + key)).lower() in   [str(x).lower() for x in values] )
						 .map(lambda line: cloudUserId_to_uuid_str(eval("line." + identifier)))
						 .distinct()
						 .map(lambda line: (line, True))
						 )
		return result
	except KeyError:
		raise KeyError("Missing Extra Parameter")	


# def load_extras_filtering(extras_list, sc):
# 	extras = {}
# 	c = 0
# 	for extra in extras_list:
# 		try:
# 			if extra["format"] == "csv":
# 				key = extra["key"]
# 				identifier = extra["identifier"]
# 				values = extra["values"]

# 				extras[c] = (sc.textFile(extra["path"])
# 							.map(lambda line: line.split(","))
# 							.filter(lambda line: str(line[key]).lower() in [str(x).lower() for x in values])
# 							.map(lambda line: (line[identifier], True))
# 							)

# 			if extra["format"] == "parquet":
# 				key = extra["key"]
# 				values = extra["values"]
# 				identifier = extra["identifier"]
# 				sqlContext = SQLContext(sc)
# 				if "installId" in identifier:
# 					installId_cloudUserId = (sc.textFile(PATH2INSTALLIDS).map(lambda line: line.split(","))
# 											   .map(lambda line: (line[0], line[1]))
# 											)

# 					extras[c] = (sqlContext.read.load(extra["path"])
# 						         .rdd
# 						         .filter(lambda line: eval("line." + key) is not None )
# 								 .filter(lambda line: str(eval("line." + key)).lower() in [str(x).lower() for x in values] )
# 								 .map(lambda line: (cloudUserId_to_uuid_str(eval("line." + identifier)), True))
# 								 .join(installId_cloudUserId)
# 								 .map(lambda line: line[1])
# 								 .map(lambda line: (line[-1], line[0]))
# 								 )

# 				else:
# 					extras[c] = (sqlContext.read.load(extra["path"])
# 						         .rdd
# 						         .filter(lambda line: eval("line." + key) is not None )
# 								 .filter(lambda line: str(eval("line." + key)).lower() in   [str(x).lower() for x in values] )
# 								 .map(lambda line: (cloudUserId_to_uuid_str(eval("line." + identifier)), True))
# 								 )
# 		except KeyError:
# 			raise KeyError("Missing Extra Parameter")
# 		c += 1
# 	return extras

def filter_query(line, filters, users_list, installId_list):
    flag = True
    for prperty, value in filters.items():
        #print prperty, value
        if prperty == "start_date":
            if not line.endTimestamp >= date_to_milliseconds(value):
                return False
        elif prperty == "end_date":
            if not line.endTimestamp <= date_to_milliseconds(value):
                return False
        elif prperty == "geoIP.location":
            locations = value
            if line.geoIP is None\
            or line.geoIP.location is None\
            or not check_location(line.geoIP.location.latitude, line.geoIP.location.longitude, locations ):
                return False
        elif prperty == "enabledLanguages":
            if line.enabledLanguages is None or len(set(value) & set(line.enabledLanguages)) == 0: 
                return False
        elif prperty == "users":
            if users_list.get(cloudUserId_to_uuid_str(line.cloudUserId), False) == False:
            	return False
        elif prperty == "installIds":
            if installId_list.get(cloudUserId_to_uuid_str(line.installId), False) == False:
            	return False
        elif prperty in ["emoji_sentence", "freq_limit", 
        				 "users_limit", "only_emojis", 
        				 "state_trend", "ngrams",
        				 "all_dataset", "extras"]:
            continue
        else:
            # if eval("line." + prperty) is None:
            #     return False
            # stages_prpty = prperty.split('.')
            if not check_if_field_is_not_none(line, prperty):	
            	return False
            # print stages_prpty
            # if len(stages_prpty) == 4:
            #     if eval("line." + stages_prpty[0]) is None\
            #         or eval("line." + '.'.join(stages_prpty[:1])) is None\
            #         or eval("line." + '.'.join(stages_prpty[:2])) is None\
            #         or eval("line." + '.'.join(stages_prpty[:3])) is None:
            #             return False

            # if len(stages_prpty) == 3:
            #     if eval("line." + stages_prpty[0]) is None\
            #         or eval("line." + '.'.join(stages_prpty[:1])) is None\
            #         or eval("line." + '.'.join(stages_prpty[:2])) is None:
            #             return False

            # if len(stages_prpty) == 2:
            #     if eval("line." + stages_prpty[0]) is None\
            #         or eval("line." + '.'.join(stages_prpty[:1])) is None:
            #             return False

            # if len(stages_prpty) == 1:
            #     if eval("line." + stages_prpty[0]) is None:
            #             return False
            if not check_filter(eval("line." + prperty), value):
            #if not check_query_in_data(value, eval("line." + prperty)):
                return False
    return True 
    

def first_stage_filtering(data, list_of_queries, filters, sc):
	if "users" in filters:
		users_list = (sc.textFile(filters["users"]).map(lambda line: (line.split(",")[0], True)).collectAsMap())
	else:
		users_list = None

	if "installIds" in filters:
		installId_list = (sc.textFile(filters["installIds"]).map(lambda line: (line.split(",")[0], True)).collectAsMap())
	else:
		installId_list = None

	return (data
				.filter(lambda line: filter_query(line, filters, users_list, installId_list))
				.filter(lambda line: checkTermsInLm(list_of_queries, PidginModel(bytes(line.pidginData)).terms, filters))
				#.filter(lambda line: check_query_in_data(list_of_queries, PidginModel(bytes(line.pidginData)).terms, filters = filters))
			)

def second_stage_filtering(data, filters, stopwords, nltk_tokenizer = False):
	return (data
				.filter(lambda line: line[1] >= filters.get("freq_limit", 25) and line[2] >= filters.get("users_limit", 25))
				.filter(lambda line: set(tokenizer(line[0][0], nltk_tokenizer = nltk_tokenizer)).issubset(set(stopwords)) == False)
				.filter(lambda line: remove_web_searches_spam(line[0][0]))
			)


def remove_web_searches_spam(token):
    token = token.replace(" ","")
    return (re.search(u'(http)|(20&)|(://)|(%)|(/)|(=)|(\^uri)|(\.)', token) is None)



def check_filter(line, values):
    line = line.lower()
    for value in values:
    	value = value.lower()
        if "regex" in value:
            value = value.split(":")[-1]
            if re.search(value,line):
                return True
        if line == value:
            return True
    return False

