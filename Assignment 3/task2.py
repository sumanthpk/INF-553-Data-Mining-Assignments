import os
from pyspark import SparkContext
import json,re
import sys
# from itertools import combinations
import time
import random
import itertools
import math
# .chain.from_iterable as from_iterable


# output_file = sys.argv[2]
start_time = time.time()

def text_preprocess(x,stopwords):
    words = []
    for text in x[1]:
        text = re.sub(r'[^\w\s]','',text)
        text = text.replace('\n', " ")
        text = text.split(' ')

    for word in text:
        word = word.strip().lower()
        if (word.isnumeric()):
            continue
        if (word not in stopwords):
            words.append(word)

    return (x[0],words)

def func1(x):
    # print(len(x[1]))
    res = []
    dict = {}
    for word in x[1]:
        if (word in dict):
            dict[word] += 1
        else:
            dict[word] = 1
    for key in list(dict.keys()):
        res.append((key,([x[0]],dict[key])))

    return res

def func2(x):
    res = []
    for word in x[1][0]:
        res.append((word,(x[0],x[1][1])))
    return res

def counting(x,total_count):
    dict = {}
    res = []
    for word in x[1]:
        if (len(word[0]) < 3 or word[0] == ''):
            continue
        if ((word[1]/total_count) > 0.000001):
            res.append(word[0])

    return (x[0],res)

def term_freq(x):
    dict = {}
    res = []
    for word in x[1]:
        if (word in dict):
            dict[word] += 1
        else:
            dict[word] = 1
    for key in list(dict.keys()):
        res.append((key,([(x[0],dict[key]/len(x[1]))],1)))
    return res

def func3(x):
    res = []
    for doc in x[1][0]:
        res.append((doc[0],(x[0],doc[1],x[1][1])))
    return res

def tf_idf(x,doc_count):

    words = []
    for word in x[1]:
        words.append((word[0],(word[1]*math.log(doc_count/word[2]))))

    words.sort(key = lambda x : x[1],reverse = True)

    if (len(words) > 200):
        words = words[:200]
    res = []
    for word in words:
        res.append(word[0])

    return (x[0],res)

def business_profile(x,tfidf):
    res = []
    for word in x[1]:
        res.append(tfidf[word])
    return (x[0],res)

def user_profile(x,tfidf):
    res = set()

    for word in x[1]:
        if (word in tfidf):
            res.add(tfidf[word])

    res = list(res)

    return (x[0],res)



if __name__ == "__main__":
    sc = SparkContext("local[*]",'task1')
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    model_file = sys.argv[2]
    read_data = sc.textFile(input_file).map(json.loads)
    bus_data = read_data.map(lambda x : (x["business_id"],x["text"])).groupByKey()

    file = open(sys.argv[3],'r')
    stop_words = set()
    for word in file:
        stop_words.add(word.rstrip())

    doc_rdd = bus_data.map(lambda x : text_preprocess(x,stop_words))
    doc_count = doc_rdd.count()

    word_count = doc_rdd.flatMap(lambda x : x[1]).map(lambda x: (x,1))
    total_count = word_count.count()
    doc_rdd = doc_rdd.flatMap(func1).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])).flatMap(func2).groupByKey()\
                                                                                .map(lambda x : counting(x,total_count))

    doc_rdd = doc_rdd.flatMap(term_freq).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])).flatMap(func3).groupByKey()\
                                                                                .map(lambda x : tf_idf(x,doc_count))

    tfidf_words = doc_rdd.flatMap(lambda x : x[1]).sortBy(lambda x: x).distinct().collect()

    tfidf_dict = {}
    i =0
    for word in tfidf_words:
        if word not in tfidf_dict:
            tfidf_dict[word] = i;
            i= i+1

    # Business Profile
    bus_profile = doc_rdd.map(lambda x : business_profile(x,tfidf_dict)).collectAsMap()

    #User Profile

    us_profile = read_data.map(lambda x : (x["user_id"],x["text"])).groupByKey().map(lambda x : text_preprocess(x,stop_words)).\
                                                                                map(lambda x : user_profile(x,tfidf_dict)).collectAsMap()

    with open(model_file,'w') as outfile:
        json.dump((bus_profile,us_profile,len(tfidf_dict.keys())),outfile)
