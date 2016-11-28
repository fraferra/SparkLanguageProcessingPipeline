import constants
#from disambiguation import *
from nltk.tokenize import word_tokenize
import re
import numpy as np
from scipy.spatial.distance import *

def alpha_numeric(word):
	return re.match('^[\w-]+$', word) is not None or word in [":)",":(",")-:",":-)","(:","):"]

def get_merged_vec(words, vecs, query, stopwords, num_features = 300):
    if type(words) == str:
        words = word_tokenize(words)
    # Function to average all of the word vectors in a given
    # paragraph
    #
    # Pre-initialize an empty numpy array (for speed)
    featureVec = np.zeros((num_features,),dtype="float32")
    #
    nwords = 0.
    # 
    # Index2word is a list that contains the names of the words in 
    # the model's vocabulary. Convert it to a set, for speed 
    #
    # Loop over each word in the review and, if it is in the model's
    # vocaublary, add its feature vector to the total
    for word in words:
        if word[0] == "-":
            tmp_word = word[1:].lower()
        else:
            tmp_word = word.lower()
        
        if vecs.value.get(tmp_word, None) is not None and tmp_word not in query and tmp_word not in stopwords and alpha_numeric(tmp_word): 
            #print word
            if word[0] == "-":
                v = -1.0*vecs.value[word[1:].lower()]
            else:
                v = +1.0*vecs.value[word.lower()]
                
            nwords = nwords + 1.
            featureVec = np.add(featureVec,v)
    # Divide the result by the number of words to get the average
    if nwords == 0.:
        nwords = 1.
    featureVec = np.divide(featureVec,nwords)
    return featureVec

def get_sentiment(sentence, sentiment_vector, word2vecs, stopwords, word = []):
	list_queries = [x.replace("_", " ").split() for x in word]
	list_queries = [item for sublist in list_queries for item in sublist]

	pos = is_same_context_2(sentence, sentiment_vector["POS"], list_queries, stopwords, word2vecs)
	neg = is_same_context_2(sentence, sentiment_vector["NEG"], list_queries, stopwords, word2vecs)

	return choose_sentiment(pos, neg)


def is_same_context_2(sentence, vec_keywords, query, stopwords, word2vecs):
    vec_sentence = get_merged_vec(sentence, word2vecs, query, stopwords)
    dist = cosine(vec_sentence, vec_keywords)
    #print dist
    return dist


def get_distance(sentence, vec_keywords, query, stopwords, word2vecs):
	vec_sentence = get_merged_vec(sentence, word2vecs, query, stopwords)
	dist = cosine(vec_sentence, vec_keywords)
	#print dist
	# sys.stderr.write(",".join([str(x) for x in vec_sentence]) + "\n")
	# sys.stderr.write(str(dist) + "\n")
	return dist

def choose_sentiment(pos, neg):
    if pos > 0.5 and neg > 0.5:
        return "NEUTRAL"
    else:
        if pos < neg:
            return "POSITIVE"
        else:
            return "NEGATIVE"