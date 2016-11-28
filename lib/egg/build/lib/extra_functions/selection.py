import re
import os
from nltk.tokenize import word_tokenize

def tokenizer(sentence, nltk_tokenizer = False):
	if nltk_tokenizer:
		return word_tokenize(sentence)
	else:
		return sentence.split()

def checkTermsInSentence(list_of_queries, sentence):
	flag = False
	sentence = sentence.lower()
	for query in list_of_queries:
		query = query.lower()
		if "regex" in query:
			query = query.replace('regex:','').replace('_',' ')
			if query in sentence:
				flag = True
			else:
				if not all(item == None for item in [re.match(query,term) for term in tokenizer(sentence)]):
					flag = True
		else:
			if len(query.split('_'))>1:
				flag =  checkMultipleWords(query, sentence)
			else:
				flag = checkBagWords(query, tokenizer(sentence))
		if flag:
			return True
	return False


def check_emoji_in_terms(terms):
	if type(terms) == list:
		for word in terms:
			if is_emoji(word):
				return True
		return False
	else:
		sys.stderr.write("terms must in be in list")

def checkBagWords(q, terms):
	if set(q.split()).issubset(set(terms)):
		return True
	else:
		return False


def checkMultipleWords(query, sentence):
	for i in range(0,len(tokenizer(sentence))-1):
		if sentence.split()[i : i+len(query.split('_'))] == query.split('_'):
			return True
	return False


def is_emoji(token):
	try:
	    return (re.match(u'[\U0001F300-\U0001F64F]', token) is not None) or \
	           (re.match(u'[\U0001F680-\U0001F6FF]', token) is not None) or \
	           (re.match(u'[\u2600-\u26FF]', token) is not None) or \
	           token in [u'\u2764',u'\u25CF',u'\u270c',u'\u2744', \
	                     u'\u2764\ufe0f',u'\u2708',u'\u2b50', \
	                     u'\u231a',u'\u2702',u'\u270c\ufe0f', \
	                     u'\u2734',u'\u2020']
	except TypeError:
		raise TypeError("String Needed")


def check_query_in_data(list_of_queries, data, filters = {}, is_sentence = False):
	if type(data) == str:
		data = tokenizer(sentence)
		print data
		is_sentence = True
	emoji_sentence = filters.get("emoji_sentence", "false").lower() == "true"
	all_sentence = filters.get("all_dataset", "false").lower() == "true"	

	terms = [x.lower() for x in data]
	
	if all_sentence:
		return True

	if emoji_sentence:
		if check_emoji_in_terms(terms):
			return True

	
	flag = False
	for query in list_of_queries:
		query = query.lower()
		# if emoji_sentence:
			# if not all(item == False for item in [is_emoji(term) for term in terms]):
			# 	flag = True
			# 	break	
		if "regex" in query:
			query = query.replace('regex:','').replace('_',' ')
			if query in terms:
				flag = True
			else:
				if not all(item == None for item in [re.match(query,term) for term in terms]):
					flag = True
		else:
			if len(query.split('_'))>1:
				if is_sentence:
					flag =  checkMultipleWords(query, " ".join(terms))
				else:
					if set(query.split('_')).issubset(set(terms)):
						flag = True		
			else:
				flag = checkBagWords(query, terms)
		if flag:
			return True
	return False	

def checkTermsInLm(list_of_queries, terms, filters, strict = False):
	"""
	used to check if term in pidginData
	"""
	emoji_sentence = filters.get("emoji_sentence", "false").lower() == "true"
	all_sentence = filters.get("all_dataset", "false").lower() == "true"
	if all_sentence:
		return True

	if emoji_sentence:
		if check_emoji_in_terms(terms):
			return True

	flag = False 
	terms = [x.lower() for x in terms]
	#terms = [x for x in terms]
	for q in list_of_queries:
		q = q.lower()
		# if emoji_sentence:
		# 	if not all(item == False for item in [is_emoji(term) for term in terms]):
		# 		flag = True
		# 		break			
		if "regex" in q:
			q = q.replace('regex:','').replace('_',' ')
			if not all(item == None for item in [re.match(q,term) for term in terms]):
				flag = True
				break
		else:
			if strict:
				if q.lower().replace("_", " ") == terms[0].lower().replace("_", " "):
					flag=True
					break
			else:
				#if set(q.lower().split()).issubset(set(terms)):
				if set(q.split()).issubset(set(terms)):
					flag=True
					break
				#if set(q.lower().split('_')).issubset(set(terms)):
				if set(q.split('_')).issubset(set(terms)):
					flag=True
					break
	return flag