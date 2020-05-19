import os
from pyspark import SparkContext
import json,re
import sys
from itertools import combinations
import time
import random
import itertools
import math


def func1(x):
    res = []

    for pair in combinations(x[1], 2):
        res.append(tuple(sorted(pair)))
    return res

    # bus_list = list(x[1])
    # res = []
    # user = x[0]
    #
    # for i in range(len(bus_list)):
    #     temp = []
    #     for j in range(len(bus_list)):
    #         if( i != j):
    #             temp.append((bus_list[j],user))
    #     if (len(temp) != 0):
    #         res.append((bus_list[i],temp))
    #
    # return res

def func2(x):

    bus_1 = x[0]
    lst = list(x[1])
    res = []
    dict = {}

    for i in range(len(lst)):
        if (lst[i][0] not in dict):
            temp = []
            temp.append(lst[i][1])
            dict[lst[i][0]] = temp
        else:
            dict[lst[i][0]] = list()
            dict[lst[i][0]].append(lst[i][1])

    for key in list(dict.keys()):
        # print(len(dict[key]))
        if (len(dict[key]) >= 2):
            res.append(tuple(sorted((bus_1,key))))
            print(tuple(sorted((bus_1,key))))

    return res

def func3(x,user_dict):
    lst = list(x[1])
    usr = set()
    res = []
    sum_stars = 0

    for ele in lst:
        if (ele[0] not in usr):
            res.append((user_dict[ele[0]],ele[1]))
            usr.add(ele[0])
            sum_stars = sum_stars + ele[1]
    #
    avg = sum_stars/(len(usr))
    #
    for i in range(len(res)):
        temp = list(res[i])
        temp[1] = temp[1] - avg
        res[i] = tuple(temp)

    return (x[0],res)


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
    sc = SparkContext("local[*]",'task3')
    sc.setLogLevel("ERROR")
    input_file = sys.argv[1]
    model_file = sys.argv[2]

    read_data = sc.textFile(input_file).map(json.loads)
    bus_data = read_data.map(lambda x : (x["user_id"],x["business_id"])).groupByKey().map(lambda x : (x[0],list(set(x[1]))))
    user_count = bus_data.count()

    # bus_pair = bus_data.flatMap(func1).groupByKey().map(lambda x : (x[0],list(x[1])))\
    #                             .map(lambda x : (x[0],[j for i in x[1] for j in i]))\
    #                             .flatMap(func2).distinct()

    usr_rating = read_data.map(lambda x : (x["user_id"],x["business_id"],x["stars"]))\
                        .map(lambda x: (x[1], (x[0], x[2]))).groupByKey()\
                        .map(lambda x: (x[0], list(x[1]))).collectAsMap()

    bus_user = read_data.map(lambda x : (x["business_id"],x["user_id"])).groupByKey()\
                                    .map(lambda x : (x[0],list(set(x[1])))).collectAsMap()

    res = bus_data.flatMap(lambda x : list(combinations(x[1], 2))).distinct()\
                                .map(lambda x : co_rated(x,bus_user)).filter(lambda x : x[2] >= 3)\
                                .map(lambda x : pearson(x,usr_rating)).filter(lambda x: x[2] == 'Nan' or x[2] > 0).collect()

    print(len(res))

    outfile = open(model_file, 'w')

    for pnt in res:
        temp = dict()
        temp["b1"] = pnt[0]
        temp["b2"] = pnt[1]
        temp["sim"] = pnt[2]
        json.dump(temp, outfile)
        outfile.write("\n")

    end_time = time.time()
    print(end_time - start_time)
