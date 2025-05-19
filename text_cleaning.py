import re
import string
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import Counter
from wordcloud import WordCloud

# NLP libraries
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
from nltk.util import ngrams
from sklearn.feature_extraction.text import TfidfVectorizer
from dotenv import load_dotenv

# Download necessary NLTK resources
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
load_dotenv(override=True)

def clean_text(text):
    text = text.lower()
    text = re.sub(r'https?://\S+|www\.\S+', '', text)
    text = re.sub(r'<.*?>', '', text)
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(r'\d+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text
    
# these two steps are together
def tokenize_text(text):
    return word_tokenize(text)
def remove_stopwords(tokens, custom_stopwords=None):
    stop_words = set(stopwords.words('english'))
    
    # Add custom stopwords if provided
    if custom_stopwords:
        stop_words.update(custom_stopwords)
    
    return [token for token in tokens if token not in stop_words]

# main format of the words.
def lemmatize_tokens(tokens):
    """Reduce tokens to their lemma form (dictionary form)"""
    lemmatizer = WordNetLemmatizer()
    return [lemmatizer.lemmatize(token) for token in tokens]

def count_words(tokens):
    """Count frequency of each token"""
    #longer tokens
    tk = [t for t in tokens if len(t)> 2 ]
    return Counter(tk)
