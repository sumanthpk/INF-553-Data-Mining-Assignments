import os
from pyspark import SparkContext
import json,re
import sys
import time
import binascii
import random
import csv
start_time = time.time()
sc = SparkContext("local[*]",'test')
sc.setLogLevel("OFF")


def get_hash(k=5):

    # Define hash function here
    a = random.sample(range(1, 100), k)
    b = random.sample(range(1, 100), k)

    m = 6020

    p = 6029


    hash_funcs =[[int(a[i]),int(b[i]),p,m] for i in list(range(0, k))]

    return hash_funcs


if __name__ == "__main__":
    start_time = time.time()

    input_file = sys.argv[1]
    test_file = sys.argv[2]
    outfile = sys.argv[3]
    csv_file = open(outfile, 'w')
    csv_write = csv.writer(csv_file, delimiter=' ', lineterminator='\n')
    read_data = sc.textFile(input_file).map(json.loads)


    # business-id
    read_data = read_data.map(lambda x : (x['city'],1)).groupByKey().map(lambda x : x[0])\
                                                            .filter(lambda x : x)\
                                                            .map(lambda x : int(binascii.hexlify(x.encode('utf8')),16))


    city_count = read_data.count()
    bit_array = [0]*(city_count*7)

    k =5

    hashs = get_hash(k)

    arr_ind = read_data.flatMap(lambda x : ([((h[0]*x + h[1])%h[2])%h[3] for h in hashs])).map(lambda x : (x,1))\
                                                                                        .groupByKey().map(lambda x : x[0])\
                                                                                        .collect()

    for ind in arr_ind:
        bit_array[ind] = 1

    f = open(test_file, "r")
    res = []
    for line in f:
        js = json.loads(line)
        city = js['city']
        if (len(city) == 0):
            res.append(0)
        else:
            num = int(binascii.hexlify(city.encode('utf8')), 16)
            ind = [((h[0] * num + h[1]) % h[2]) % h[3] for h in hashs]
            if (sum([bit_array[i] for i in ind]) != k):
                res.append(0)
            else:
                res.append(1)

    csv_write.writerow(res)
    end_time = time.time()
    print(end_time - start_time)
