
import os
from pyspark import SparkContext
import json,re
import sys
import time
import random
import itertools
import math


def cosin_similarity(vec1,vec2):
    num = 0
    v1_sum = 0
    v2_sum = 0

    for i in range(len(vec1)):
        num = num + vec1[i] * vec2[i]
        v1_sum = v1_sum + vec1[i]
        v2_sum = v2_sum + vec2[i]

    simi = (num)/(math.sqrt(v1_sum)*math.sqrt(v2_sum))
    return simi;


def predict(x,file):
    u_id = x['user_id']
    b_id = x['business_id']
    b_vec = [0]*file[2]
    u_vec = [0]*file[2]

    try:
        b_words = file[0][b_id]
        u_words = file[1][u_id]
    except:
        return(u_vec,b_vec,0)

    if (len(u_words) == 0 or len(b_words) == 0):
        return(u_vec,b_vec,0)

    for word in b_words:
        b_vec[word] = 1;

    for word in u_words:
        u_vec[word] = 1;

    simi = cosin_similarity(b_vec,u_vec)
    return (u_id,b_id,simi);




test_file = sys.argv[1]
model_file = sys.argv[2]
out_file = sys.argv[3]

with open(model_file) as file:
    file = json.load(file)
sc = SparkContext("local[*]",'task1')
sc.setLogLevel("ERROR")
test_data = sc.textFile(test_file).map(json.loads).map(lambda x: predict(x,file))\
                                        .filter(lambda x : x[2]>= 0.01).collect()

outfile = open(out_file,'w')

for pnt in test_data:
    temp = dict()
    temp["user_id"] = pnt[0]
    temp["business_id"] = pnt[1]
    temp["sim"] = pnt[2]
    json.dump(temp,outfile)
    outfile.write("\n")
