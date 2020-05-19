import os
from pyspark import SparkContext
import json,re
import sys
from itertools import combinations
import time
import random
import itertools
import math

start_time = time.time()

def list2dict(ls):
    dict = {}
    ind = 0
    for l in ls:
        dict[l] = ind
        ind = ind + 1
    return dict


def get_hash(user_rows, m):
    hash_row = []
    # Define hash function here
    n = 35
    a = list(range(1, n))
    b = list(range(5 * n, 7 * n))

    p = [100003, 100019, 100043, 100049, 100057, 100069, 100103, 100109, 100129, 100151]

    for hash in list(range(0, n)):
        h = [a[hash % (len(a))], b[hash % (len(b))], p[hash % (len(p))]]
        hash_row.append(min([(((h[0] * user + h[1]) % h[2]) % m) for user in user_rows]))
    return hash_row


def get_signature(x, dict, m):
    users = x[1]
    user_rows = [dict[user] for user in users]
    hash_row = get_hash(user_rows, m);
    return (x[0], hash_row)


def band(x):
    n = 35
    b = 35
    r = int(n / b)
    sigs = x[1]
    bands = []
    for i in range(b):
        keys = sigs[i * r:(i + 1) * r]
        if (len(keys) == 1):
            keys = keys[0]
        else:
            keys = tuple(keys)
        temp = ((i, keys), x[0])
        bands.append(temp)
    return bands


def jaccard(x, bus_usr_dict):
    set_1 = set(bus_usr_dict[x[0]])
    set_2 = set(bus_usr_dict[x[1]])
    intersection = len(set_1.intersection(set_2))
    union = len(set_1.union(set_2))
    sim = intersection / union
    return (x[0], x[1], sim)


def get_pairs(x):
    res = []
    for pair in combinations(x[1], 2):
        res.append(tuple(sorted(pair)))
    return res


def pearson(x, bus_dict):
    bus_1 = bus_dict[x[0]]
    bus_2 = bus_dict[x[1]]

    u_1 = dict()
    for ele in bus_1:
        if (ele[0] not in u_1):
            u_1[ele[0]] = [ele[1], 1]
        else:
            u_1[ele[0]][0] = u_1[ele[0]][0] + ele[1]
            u_1[ele[0]][1] = u_1[ele[0]][1] + 1
    avg_1 = 0
    for key in list(u_1.keys()):
        u_1[key] = u_1[key][0] / u_1[key][1]
        avg_1 = avg_1 + u_1[key]
    avg_1 = avg_1 / len(list(u_1.keys()))

    u_2 = dict()
    for ele in bus_2:
        if (ele[0] not in u_2):
            u_2[ele[0]] = [ele[1], 1]
        else:
            u_2[ele[0]][0] = u_2[ele[0]][0] + ele[1]
            u_2[ele[0]][1] = u_2[ele[0]][1] + 1
    avg_2 = 0
    for key in list(u_2.keys()):
        u_2[key] = u_2[key][0] / u_2[key][1]
        avg_2 = avg_2 + u_2[key]
    avg_2 = avg_2 / len(list(u_2.keys()))

    norm_1 = 0
    norm_2 = 0
    dict1 = {}
    for ele in bus_1:
        norm_rating = u_1[ele[0]] - avg_1
        dict1[ele[0]] = norm_rating
        norm_1 = norm_1 + (norm_rating * norm_rating)

    num = 0
    for ele in bus_2:
        norm_rating = u_2[ele[0]] - avg_2
        if ele[0] in dict1:
            num = num + norm_rating * dict1[ele[0]]
        norm_2 = norm_2 + (norm_rating * norm_rating)

    try:
        sim = round(num / (math.sqrt(norm_1) * math.sqrt(norm_2)), 5)
    except:
        sim = 'Nan'

    return (x[0], x[1], sim)

def co_rated(x, bus_usr_dict):
    set_1 = set(bus_usr_dict[x[0]])
    set_2 = set(bus_usr_dict[x[1]])
    intersection = len(set_1.intersection(set_2))
    return (x[0], x[1], intersection)

if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext("local[*]", 'task3')
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    model_file = sys.argv[2]

    read_data = sc.textFile(input_file).map(json.loads).map(lambda x: (x["business_id"], x["user_id"], x["stars"]))
    usr_bus = read_data.map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], set(x[1])))
    usr_bus_dict = usr_bus.collectAsMap()

    usr_rating = read_data.map(lambda x: (x[1], (x[0], x[2]))).groupByKey() \
        .map(lambda x: (x[0], list(x[1]))).collectAsMap()

    # Businesses in a list.
    bus = read_data.map(lambda x: x[0]).distinct().collect()
    bus_dict = list2dict(bus)
    m = len(bus)

    # Min Hashing (Signature Matrix)
    sign_matrix = usr_bus.map(lambda x: get_signature(x, bus_dict, m))

    # LSH
    cands = sign_matrix.flatMap(band).groupByKey().map(lambda x: (x[0], list(x[1]))) \
        .filter(lambda x: len(x[1]) > 1) \
        .flatMap(get_pairs).map(lambda x: (x, 1)) \
        .groupByKey().map(lambda x: x[0])

    # result
    res = cands.map(lambda x: co_rated(x, usr_bus_dict)).filter(lambda x: x[2] >= 3) \
        .map(lambda x: jaccard(x, usr_bus_dict)).filter(lambda x: x[2] >= 0.01) \
        .map(lambda x: pearson(x, usr_rating)).filter(lambda x: x[2] == 'Nan' or x[2] > 0).collect()

    print(len(res))


    #
    # # Business in a list
    # bus_data = user_bus.map(lambda x: (x[1],1)).groupByKey().sortByKey().map(lambda x: x[0]).collect()
    # m = len(bus_data)  # num of businesses
    # bus_dict = list2dict(bus_data)
    # del bus_data
    #
    # user_sig = user_bus.groupByKey().map(lambda x : (x[0],set(list(x[1])))).map(lambda x : get_signature(x,bus_dict,m))
    # user_sig_matrix = user_sig.collect()
    # cands  = user_sig.flatMap(band).groupByKey().filter(lambda x : len(x[1])> 1)\
    #                                                         .flatMap(lambda x : list(combinations(x[1],2)))\
    #                                                         .map(lambda x : (x,1)).groupByKey().map(lambda x: x[0])
    #
    # user_data = user_bus.map(lambda x : x[0]).distinct().collect()
    #
    #
    # user_dict = list2dict(user_data)
    # user_bus_dict = user_bus.groupByKey().map(lambda x : (x[0],(list(x[1])))).collectAsMap()
    # res = cands.map(lambda x: jaccard(x, user_dict, user_sig_matrix)).filter(lambda x: x[2] >= 0.01)\
    #                                         .map(lambda x : (x[0],x[1])).map(lambda x : user_pairs(x,user_bus_dict))\
    #                                                                 .filter(lambda x : x[2] == 1).map(lambda x : (x[0],x[1]))#.count()
    #
    # user_avg = read_data.map(lambda x: (x["user_id"], (x["business_id"],x['stars']))).groupByKey()\
    #                                                         .map(lambda x : func3(x,bus_dict)).collectAsMap()
    #
    # final = res.map(lambda x : pearson(x,user_avg)).filter(lambda x: (x[1] == 'Nan' or x[1] > 0)).collect()
    #
    #
    # print(len(final))

    # del user_bus_dict
    # del user_dict
    #
    #








