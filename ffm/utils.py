#!/usr/bin/env python
# -*- coding: utf-8 -*- 


__author__ = "Theo"


"""--------------------------------------------------------------------
NATURAL LANGUAGE PROCESSING FUZZY MATCHING FUNCTIONS
Grouping various scripts and functions for nlp 

Started on the 02/06/2017


Distance possible: 
Levenshtein distance
Damerau-Levenshtein distance
Needleman–Wunsch algorithm
Spell-checker method
Ratcliff/Obershelp algorithm

http://chairnerd.seatgeek.com/fuzzywuzzy-fuzzy-string-matching-in-python/


theo.alves.da.costa@gmail.com
https://github.com/theolvs
------------------------------------------------------------------------
"""


import numpy as np
import pandas as pd
from tqdm import tqdm
import scipy
import time
import re
import string

# NLTK
import nltk
from nltk.metrics.distance import jaccard_distance
from nltk.util import ngrams

# FUZZYWUZZY
from fuzzywuzzy import fuzz,process

# GENSIM
import warnings
import gensim
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')
from gensim.models.word2vec import Word2Vec

# SKLEARN
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors




#=============================================================================================================================
# FAST FUZZY MATCHING UTILS
# Optimized version by Theo ALVES DA COSTA
#=============================================================================================================================



def create_ngrams(s,n = 3):
    """Create N Grams tokens from a string
    """
    return ["".join(x) for x in ngrams(s,n)]



def tokenize_ngrams(texts,n = 3,**kwargs):
    """Tokenize with N grams methods a list of strings
    """
    return [create_ngrams(text,n = n,**kwargs) for text in texts]


def set_unique(sequence):
    """Ordered unique list of occurences helper function using sets 
    """
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]



def create_token2vec_model(tokens,size = 100,iter = 500,min_count = 2,**kwargs):
    """Token2Vec model creation using gensim Word2Vec algorithm
    """
    print(">> Creating Token2Vec model",end = "")
    t2v = Word2Vec(size = size,min_count=min_count,iter = iter,**kwargs)
    t2v.build_vocab(tokens,keep_raw_vocab=True)
    t2v.train(tokens,total_examples=t2v.corpus_count, epochs=t2v.iter)
    print(" - ok")
    return t2v



def create_tfidf_model(texts,tokenizer = lambda x : x.split("|")):
    """TfIdf model creation using Scikit Learn TfIdf vectorizer
    """
    print(">> Creating TfIdf model",end = "")
    vectorizer = TfidfVectorizer(lowercase=False,tokenizer = tokenizer)
    tfidf = vectorizer.fit_transform(texts).todense()
    tfidf = pd.DataFrame(tfidf,columns = [x[0].upper() for x in sorted(vectorizer.vocabulary_.items(),key = lambda x : x[1])])
    tfidf = tfidf.loc[:,~tfidf.columns.duplicated()]
    print(" - ok")
    return vectorizer,tfidf





#=============================================================================================================================
# HELPERS
#=============================================================================================================================



def sec_to_hms(seconds):
    """Time displaying function helper
    """
    if seconds < 60:
        return "{}s".format(int(seconds))
    elif seconds < 60*60:
        m,s = divmod(int(seconds),60)
        return "{}m{}s".format(m,s)
    else:
        m,s = divmod(int(seconds),60)
        h,m = divmod(m,60)
        return "{}h{}m{}s".format(h,m,s)







#=============================================================================================================================
# CLASSICAL FUZZY MATCHING
#=============================================================================================================================



def fuzzy_match(X,Y,threshold = 60):
    """
    Fuzzy matching between two elements

    :param list X: the input list or element to be matched
    :param list Y: the target list to be matched on
    :param int or list threshold: the threshold to consider under which the similarity will yield a None
    :returns: pandas.DataFrame -- each element of X matched (or the best match for an single element)
    """

    if type(threshold) != list:

        # Case single element
        if type(X) != list:
            choice = process.extract(X,Y)[0][0]
            match = fuzz.token_sort_ratio(X,choice)
            if match > threshold:
                return choice
            else:
                return None
        else:
            tqdm.pandas(desc="Fuzzy matching at threshold {}%".format(threshold))
            data = pd.DataFrame(X,columns = ["input"])
            data["match_{}%".format(threshold)] = data["input"].progress_map(lambda x : fuzzy_match(x,Y,threshold = threshold))
            return data

    else:
        data = pd.DataFrame(X,columns = ["input"])
        for t in threshold:
            threshold_data = fuzzy_match(X,Y,threshold = t)
            data["match_{}%".format(t)] = threshold_data["match_{}%".format(t)]
        return data









#=============================================================================================================================
# REMOVE FUZZY DUPLICATES
#=============================================================================================================================


def remove_duplicates(texts,distance = None,threshold = 1):
    texts = list(set(texts))
    
    if distance is None:
        distance = levenshtein
        
    new_texts = []
    while len(texts) > 0:
        text = texts[0]
        cut = False
        for t in [t for t in texts if t != text]:
            if distance(text,t) <= threshold:
                cut = True
                break
                
        if not cut:
            new_texts.append(text)
        texts = texts[1:]
        
    return new_texts










#=============================================================================================================================
# DISTANCE
#=============================================================================================================================



def levenshtein(s, t):
    ''' 
    From Wikipedia article; Iterative with two matrix rows.
    Copied from https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python
    '''
    if s == t: return 0
    elif len(s) == 0: return len(t)
    elif len(t) == 0: return len(s)
    v0 = [None] * (len(t) + 1)
    v1 = [None] * (len(t) + 1)
    for i in range(len(v0)):
        v0[i] = i
    for i in range(len(s)):
        v1[0] = i + 1
        for j in range(len(t)):
            cost = 0 if s[i] == t[j] else 1
            v1[j + 1] = min(v1[j] + 1, v0[j + 1] + 1, v0[j] + cost)
        for j in range(len(v0)):
            v0[j] = v1[j]
            
    return v1[len(t)]


    


def levenshtein2(s, t):
    if len(s) < len(t):
        return levenshtein(t, s)

    # So now we have len(s) >= len(t).
    if len(t) == 0:
        return len(s)

    # We call tuple() to force strings to be used as sequences
    # ('c', 'a', 't', 's') - numpy uses them as values by default.
    s = np.array(tuple(s))
    t = np.array(tuple(t))

    # We use a dynamic programming algorithm, but with the
    # added optimization that we only need the last two rows
    # of the matrix.
    previous_row = np.arange(t.size + 1)
    for s in s:
        # Insertion (t grows longer than s):
        current_row = previous_row + 1

        # Substitution or matching:
        # t and s items are aligned, and either
        # are different (cost of 1), or are the same (cost of 0).
        current_row[1:] = np.minimum(
                current_row[1:],
                np.add(previous_row[:-1], t != s))

        # Deletion (t grows shorter than s):
        current_row[1:] = np.minimum(
                current_row[1:],
                current_row[0:-1] + 1)

        previous_row = current_row

    return previous_row[-1]




def jaccard(s,t):
    return jaccard_distance(set(s),set(t))



