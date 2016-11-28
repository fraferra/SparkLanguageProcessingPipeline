import numpy as np
from scipy.spatial.distance import *
from nltk.tokenize import word_tokenize
from selection import *
import constants
import sys
import re

def alpha_numeric(word):
	return re.match('^[a-zA-Z]+$', word) is not None or word in [":)",":(",")-:",":-)","(:","):"]

# def get_merged_vec(words, wordvecs, query, stopwords, num_features = 300):
#     # Function to average all of the word vectors in a given
#     # paragraph
#     #
#     # Pre-initialize an empty numpy array (for speed)
#     featureVec = np.zeros((num_features,),dtype="float32")
#     #
#     nwords = 0.

#     for word in words:
#         if word[0] == "-":
#             tmp_word = word[1:].lower()
#         else:
#             tmp_word = word.lower()

#         if wordvecs.value.get(tmp_word, None) is not None and not checkTermsInSentence(query, tmp_word) and tmp_word not in stopwords and alpha_numeric(tmp_word): 
#             nwords = nwords + 1.
#             if word[0] == "-":
#                 v = -1.0*wordvecs.value[word[1:].lower()]
#             else:
#                 v = +1.0*wordvecs.value[word.lower()]
#             featureVec = np.add(featureVec,v)
#     # 
#     # Divide the result by the number of words to get the average
#     if nwords == 0.:
#         nwords = 1.
#     featureVec = np.divide(featureVec,nwords)
#     return featureVec

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


def convert_line(line):
    try:
        return (line[0], np.array([float(x) for x in line[1:]]))
    except ValueError:
        return None   


def is_same_context(sentence, vec_keywords, query, stopwords, word2vecs):
	vec_sentence = get_merged_vec(sentence, word2vecs, query, stopwords)
	dist = cosine(vec_sentence, vec_keywords)
	# sys.stderr.write(",".join([str(x) for x in vec_sentence]) + "\n")
	# sys.stderr.write(str(dist) + "\n")
	if dist <= 0.51:
		return True
	else:
		return False
