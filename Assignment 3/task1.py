import os
from pyspark import SparkContext
import json, re
import sys
from itertools import combinations
import time
import random


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
    n = 75
    a = list(range(1, n))
    b = list(range(5 * n, 7 * n))

    p = [100003, 100019, 100043, 100049, 100057, 100069, 100103, 100109, 100129, 100151,
         100153, 100169, 100183, 100189, 100193, 100207, 100213, 100237, 100267, 100271,
         100279, 100291, 100297, 100313, 100333, 100343, 100357, 100361, 100363, 100379]

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
    n = 75
    b = 75
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


if __name__ == "__main__":
    start_time = time.time()
    sc = SparkContext("local[*]", 'task1')
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    read_data = sc.textFile(input_file).map(json.loads).map(lambda x: (x["business_id"], x["user_id"]))

    bus_usr = read_data.groupByKey().map(lambda x: (x[0], set(x[1])))
    bus_usr_dict = bus_usr.collectAsMap()

    # Users in a list.
    users = read_data.map(lambda x: x[1]).distinct().collect()
    users_dict = list2dict(users)
    m = len(users)

    # Min Hashing (Signature Matrix)
    sign_matrix = bus_usr.map(lambda x: get_signature(x, users_dict, m))

    # LSH
    cands = sign_matrix.flatMap(band).groupByKey().map(lambda x: (x[0], list(x[1]))) \
        .filter(lambda x: len(x[1]) > 1) \
        .flatMap(get_pairs).map(lambda x: (x, 1)) \
        .groupByKey().map(lambda x: x[0])

    res = cands.map(lambda x: jaccard(x, bus_usr_dict)) \
        .filter(lambda x: x[2] >= 0.05).collect()

    # print(len(res))
    outfile = open(output_file, 'w')
    for pnt in res:
        temp = dict()
        temp["b1"] = pnt[0]
        temp["b2"] = pnt[1]
        temp["sim"] = pnt[2]
        json.dump(temp, outfile)
        outfile.write("\n")
    end_time = time.time()
    print(end_time - start_time)

