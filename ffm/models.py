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
import matplotlib.pyplot as plt
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
from sklearn.neighbors import BallTree,KDTree
from sklearn.manifold import TSNE

# SCIPY
from scipy.spatial.distance import cosine

# KERAS
from keras.layers import Input, LSTM, RepeatVector
from keras.models import Model
from keras.utils import to_categorical
from keras.preprocessing import sequence

# EKIMETRICS CUSTOM LIBRARY
from ffm.utils import *




class BaseFFM(object):
    def __init__(self):
        pass



    #------------------------------------------------------------------------------
    # VISUALIZATION


    def show_tsne(self,X,figsize = (8,8)):
        """Show T-SNE visualization of the encoded space
        """

        encoded_data = self.encode(X)
        tsne = TSNE()
        xy = tsne.fit_transform(encoded_data)

        fig, ax = plt.subplots(figsize=figsize)
        ax.scatter(xy[:,0],xy[:,1])

        for i, text in enumerate(X):
            ax.annotate(text, (xy[i,0],xy[i,1]))

        plt.show()





#=============================================================================================================================
# SEQUENCE MODELLER
#=============================================================================================================================








#=============================================================================================================================
# FAST FUZZY MATCHING
# Optimized version
#=============================================================================================================================




class FastFuzzyMatching(BaseFFM):
    """Fast Fuzzy Matching class
    """

    def __init__(self):
        """Initialization
        """
        pass


    #-----------------------------------------------------------------------------------------
    # PREPROCESSING FUNCTIONS


    def _preprocess(self,X,Y = None,auto_detect = False,vocabulary = None,method = "words",max_count = None):
        """Preprocessing general function that applies the preprocess() function for each entity : 
        - Remove tokens from a given vocabulary
        - Remove tokens with automatic detections if too common
        - Remove punctuations
        """

        # Safety check
        if vocabulary is None:
            vocabulary = []

        # Auto-detect and remove most frequents tokens
        if auto_detect:

            if max_count is None:
                max_count = 20

            frequent_tokens = self.most_frequent_tokens(X,Y,method)
            frequent_tokens = frequent_tokens.loc[frequent_tokens > max_count]
            frequent_tokens = list(frequent_tokens.index)
            vocabulary.extend(frequent_tokens)

        # Preprocessing function
        def preprocess(x,vocabulary):
            return re.sub(r'|'.join(map(re.escape, list(string.punctuation) + vocabulary)), '', x).strip().upper()

        # Apply preprocessing function for each entity in X and Y
        X = [preprocess(x,vocabulary) for x in X]

        if Y is not None:
            Y = [preprocess(y,vocabulary) for y in Y]

        return X,Y




    def most_frequent_tokens(self,X,Y = None,method = "trigrams"):
        """Compute the most frequent tokens given a method (trigrams, bigrams, words)
        """
        _,_,tokens = self._tokenize(X,Y,method)
        all_tokens = [token for t in tokens for token in t]
        return pd.Series(all_tokens).value_counts()






    #-----------------------------------------------------------------------------------------
    # HELPER FUNCTIONS

    def _tokenize(self,X,Y = None,method = None,unique = True,**kwargs):
        """Tokenize the list of entities given a method (bigrams, trigrams or words)
        """

        # Default arguments corrections
        if method is None: method = ["bigrams","trigrams","words"]
        if Y is None: Y = []

        # Find unique entities between X and Y entities
        if unique:
            entities = set_unique(X+Y)
        else:
            entities = set_unique(X)+set_unique(Y)

        # Apply tokenizing method
        if type(method) != list:

            if "gram" in method:
                method = method.replace("bi","2").replace("tri","3")
                n = int(method.split("gram")[0])
                tokens = tokenize_ngrams(entities,n = n,**kwargs)
            elif "word" in method:
                tokens = [entity.split() for entity in entities]
            else:
                raise ValueError("Method not understood")

            texts = ["|".join(x) for x in tokens]

        # Multiple tokenizing method
        else:

            tokens = [self._tokenize(X,Y,method = m,unique = unique)[2] for m in method]
            tokens = [set_unique([token for tokens in method_tokens for token in tokens]) for method_tokens in zip(*tokens)]
            texts = ["|".join(x) for x in tokens]

        return entities,texts,tokens






    def _transform_tfidf(self,texts):
        """Transform the dataset to the TfIdf representation of its tokens
        """
        tfidf = self.vectorizer.transform(texts).todense()
        tfidf = pd.DataFrame(tfidf,columns = [x[0].upper() for x in sorted(self.vectorizer.vocabulary_.items(),key = lambda x : x[1])])
        tfidf = tfidf.loc[:,~tfidf.columns.duplicated()]
        return tfidf




    def _encode_e2v(self,tfidf,verbose = 0):
        """Encode the entity to its vectorized representation
        """

        if verbose: print(">> Encoding to Entity2Vec representation",end = "")

        # Select only the common tokens and reorder the matrices in the same token order
        filtered_vocab = [x for x in tfidf.columns if x in self.t2v]
        tfidf = tfidf[filtered_vocab].as_matrix()

        # Create a transfer matrix 
        P_t2v = np.array([self.t2v[x] for x in filtered_vocab])

        # Encode in the entity vectorized space
        e2v = np.dot(tfidf,P_t2v)

        if verbose: print(" - ok")

        return e2v




    def _compute_distances(self,X,Y,metric = "cosine"):
        """Compute the distance matrix between encoded representations via "cosine" metric
        """
        return scipy.spatial.distance.cdist(X,Y,metric)






    def _create_nearest_neighbors_model(self,e2v,model_type = "knn",n = 5):
        """Create a Nearest Neighbors model from encoded representation of the entities
        """
        model = NearestNeighbors(n_neighbors=n, algorithm='auto',metric = "cosine").fit(e2v)
        return model





    #-----------------------------------------------------------------------------------------
    # FUNCTIONS


    def train(self,X,Y = None,method = None,size = 100,iter = 500,min_count = 2,tokenizer = lambda x : x.split("|"),**kwargs):
        """Training function
        """

        if method is None: method = ["bigrams","trigrams","words"]

        # Start a timer
        s = time.time()

        # Tokenization
        self.entities,texts,tokens = self._tokenize(X,Y,method,unique = True)

        # Train the token2vec model
        self.t2v = create_token2vec_model(tokens,size = size,iter = iter,min_count = min_count,**kwargs)
        
        # Prepare the TfIdf model
        self.vectorizer,tfidf = create_tfidf_model(texts,tokenizer = tokenizer)

        # Encode the tfidf representation to the lower dimension representation
        self.e2v = self._encode_e2v(tfidf,verbose = 1)

        # Pre train a Nearest neighbors model
        self.nn = self._create_nearest_neighbors_model(self.e2v)

        # End the timer
        e = time.time()
        print("... Training finished in {}".format(sec_to_hms(e-s)))





    def match(self,X,Y = None,method = None,model_type = "knn",retrain = True,as_distance_matrix = False,n = 5,threshold = 0.5,**kwargs):
        """Fuzzy matching two lists of texts
        """

        if method is None: method = ["bigrams","trigrams","words"]

        _,texts,_ = self._tokenize(X,Y,method,unique = False)

        # Training phase
        if not hasattr(self,"t2v") or not hasattr(self,"vectorizer"):
            self.train(X,Y,method,**kwargs)

        # Vectorize
        tfidf = self._transform_tfidf(texts)

        # Encode
        e2v = self._encode_e2v(tfidf)

        # Separate X and Y
        lenX = len(set_unique(X))
        X_e2v = e2v[:lenX]
        Y_e2v = e2v[lenX:]

        if as_distance_matrix:
            # Compute distances
            distances = self._compute_distances(X_e2v, Y_e2v, 'cosine')
            return distances

        else:

            if retrain:
                nn = self._create_nearest_neighbors_model(Y_e2v)
            else:
                nn = self.nn


            # Find the best matches with K Nearest Neighbors algorithms
            distances,indices = nn.kneighbors(X_e2v)

            # Format the results
            find_entity = np.vectorize(lambda indice : Y[indice])
            results = np.stack([find_entity(indices),distances],axis = 2)
            columns = [y for b in range(1,n+1) for y in ("match {}".format(b),"score {}".format(b))]
            results = pd.DataFrame(results.reshape((len(results),np.prod(results.shape[1:]))),index = X,columns = columns)
            for col in results.columns:
                if "score" in col:
                    results[col] = results[col].astype(float).round(3)

            return results



            

    def most_similar(self,s,method = None,as_distance_matrix = False,n = 5,threshold = 0.5):
        """Find the most similar dataset in a trained dataset model
        """

        if method is None: method = ["bigrams","trigrams","words"]


        # Create token representation
        _,texts,_ = self._tokenize([s],method = method)

        print(texts,_)
        
        # Vectorize and encode
        tfidf = self._transform_tfidf(texts)
        e2v = self._encode_e2v(tfidf)

        if as_distance_matrix:
            # Compute distances
            distances = self._compute_distances(e2v, self.e2v, 'cosine')
            return distances
        else:

            # Find the best match with pre trained K Nearest Neighbors algorithms
            distances,indices = self.nn.kneighbors(e2v)

            # Format the results
            results = [(self.entities[x[0]],x[1]) for x in zip(indices[0],distances[0]) if x[1] < threshold]
            return results



    def encode(self,s,method = None):

        if not isinstance(s,list): s = [s]

        if method is None: method = ["bigrams","trigrams","words"]

        # Create token representation
        _,texts,_ = self._tokenize(s,method = method)
        
        # Vectorize and encode
        tfidf = self._transform_tfidf(texts)
        e2v = self._encode_e2v(tfidf)

        return e2v








#=============================================================================================================================
# NEURAL FAST FUZZY MATCHING
# Optimized version
#=============================================================================================================================



class NeuralFFM(BaseFFM):
    """Neural FFM Wrapper
    """
    def __init__(self,X = None,latent_dim = 256,maxlen = 3):
        """Initialization
        """
        self.latent_dim = latent_dim
        self.maxlen = maxlen

        self.prepare_vocab(X)




    #------------------------------------------------------------------------------
    # VOCABULARY


    def prepare_vocab(self,X = None):
        """Prepare the vocabulary required for training the model
        """
        if X is not None:
            vocab = self.get_unique_vocab(X)
            self.create_matching_table(vocab)
            self.input_dim = len(self.index_vocab)



    def get_unique_vocab(self,X):
        """Extract unique vocabulary from a list of entities
        """
        vocab = set()
        for v in X:
            vocab.update(set(v))
        return vocab


    def create_matching_table(self,vocab):
        """Create a matching dictionary from a list of elements in a vocabulary
        """
        self.index_vocab = {i:v for i,v in enumerate(vocab)}
        self.vocab_index = {v:i for i,v in enumerate(vocab)}




    #------------------------------------------------------------------------------
    # DATA PREPARATION

    def vectorize(self,text,maxlen = None):
        """Vectorize a string to a LSTM one hot encoded version
        """
        if maxlen is None: maxlen = self.maxlen

        sentences = [text[i:i+maxlen] for i in range(len(text)-maxlen + 1)]
        x = np.zeros((len(sentences),maxlen,self.input_dim),dtype = np.int8)
        for i,sent in enumerate(sentences):
            for t,char in enumerate(sent):
                x[i,t,self.vocab_index[char]] = 1
        return x


    def prepare_data(self,X):
        """Vectorize a list of strings
        """
        data = np.vstack([self.vectorize(x,self.maxlen) for x in X])
        return data







    #------------------------------------------------------------------------------
    # ML MODEL


    def create_model(self):
        """Create the LSTM autoencoder model
        """
        inputs = Input(shape=(self.maxlen, self.input_dim))
        encoded = LSTM(self.latent_dim)(inputs)

        decoded = RepeatVector(self.maxlen)(encoded)
        decoded = LSTM(self.input_dim, return_sequences=True)(decoded)

        self.autoencoder = Model(inputs, decoded)
        self.encoder = Model(inputs, encoded)

        self.autoencoder.compile(loss='binary_crossentropy',optimizer='Adam')





    def train(self,data,batch_size = 32,epochs = 20,verbose = 2,**kwargs):
        """Train the autoencoder
        """
        if type(data) == list and type(data[0]) == str:
            data = self.prepare_data(data)
        self.autoencoder.fit(data,data,batch_size = batch_size,epochs=epochs,verbose=verbose,**kwargs)




    def encode(self,x):
        """Encode a string or a list of string to the encoded space
        """
        if type(x) == list:
            data = np.vstack([self.encode(y) for y in x])
            return data
        else:
            x = self.vectorize(x)
            return self.encoder.predict(x)[0]





    def compare(self,x,y):
        """Compare two strings in the encoded space using cosine distance
        """
        x = self.encode(x)
        y = self.encode(y)
        return 1-cosine(x,y)



