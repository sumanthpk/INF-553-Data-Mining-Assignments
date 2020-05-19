import os
from pyspark import SparkContext
import json,re
import sys
from itertools import combinations
import time
import random
import itertools
import math



def func1(x,train_users,bus_pairs):

    bus_id = x[1]
    res = []
    try:
        bus_ids = train_users[x[0]]
    except:
        return (x[0], bus_id, -1)


    for ids in bus_ids:
        bus = ids[0]
        stars = ids[1]
        temp = tuple(sorted((bus_id, bus)))
        if temp in bus_pairs:
            sim = bus_pairs[temp]
            if (sim != 'Nan'):
                res.append((temp,sim,stars))

    res = sorted(res, key = lambda a : a[1], reverse = True)

    i = 0
    num = 0
    wei = 0
    for ele in res:
        if i > 5:
            break
        else:
            i = i+1
            num = num + ele[1]*ele[2]
            wei = wei + ele[1]
    try:
        rating = (num/wei)
    except:
        rating = -1

    return (x[0],bus_id,rating)


if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext("local[*]", 'task3')
    sc.setLogLevel("ERROR")
    train_file = sys.argv[1]
    test_file = sys.argv[2]
    model_file = sys.argv[3]
    output_file = sys.argv[4]
    case = sys.argv[5]

    if (case == 'item_based'):

        test_pairs = sc.textFile(test_file).map(json.loads).map(lambda x : (x['user_id'],x['business_id']))
        test_users = set(test_pairs.map(lambda x : x[0]).collect())

        train_users= sc.textFile(train_file).map(json.loads).map(lambda x: (x['user_id'], (x['business_id'],x["stars"]))).filter(lambda x : x[0] in test_users)\
                                                                            .groupByKey().collectAsMap()

        bus_pairs = sc.textFile(model_file).map(json.loads).map(lambda x : (tuple(sorted((x['b1'],x['b2']))),x['sim'])).collectAsMap()

        user_bus = test_pairs.map(lambda x: func1(x,train_users,bus_pairs)).filter(lambda x: (x[2] != -1)).collect()

        outfile = open(output_file, 'w')

        for pnt in user_bus:
            temp = dict()
            temp["user_id"] = pnt[0]
            temp["business_id"] = pnt[1]
            temp["stars"] = pnt[2]
            json.dump(temp, outfile)
            outfile.write("\n")

    if (case == 'user_based'):

        test_pairs = sc.textFile(test_file).map(json.loads).map(lambda x: (x['user_id'], x['business_id']))
        test_bus = set(test_pairs.map(lambda x: x[1]).collect())

        train_users = sc.textFile(train_file).map(json.loads)\
                                        .map(lambda x: (x['business_id'], (x['user_id'], x["stars"])))\
                                        .filter(lambda x: x[0] in test_bus) \
                                        .groupByKey().collectAsMap()

        bus_pairs = sc.textFile(model_file).map(json.loads).map(
            lambda x: (tuple(sorted((x['u1'], x['u2']))), x['sim'])).collectAsMap()

        user_bus = test_pairs.map(lambda x: func1(x, train_bus, bus_pairs)).filter(
            lambda x: (x[2] != -1)).collect()

        outfile = open(output_file, 'w')

        for pnt in user_bus:
            temp = dict()
            temp["user_id"] = pnt[0]
            temp["business_id"] = pnt[1]
            temp["stars"] = pnt[2]
            json.dump(temp, outfile)
            outfile.write("\n")

    end_time = time.time()
    print(end_time-start_time)


        #result