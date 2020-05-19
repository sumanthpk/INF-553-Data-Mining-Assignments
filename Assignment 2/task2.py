import os
from pyspark import SparkContext
import json,re
import sys
import itertools
import time

from itertools import combinations
from itertools import islice
from operator import add
from itertools import chain

start_time = time.time()
sc = SparkContext("local[*]",'test')
input_file = sys.argv[3]
k1 = int(sys.argv[1])
read_data = sc.textFile(input_file)
support = int(sys.argv[2])
output_file = sys.argv[4]

sc.setLogLevel("OFF")


def counter_singleton(bucks,support,num_partitions):  # x : data in patition;
    count = {}
    res = []

    for buck in bucks:
        for cand in buck:
            if (count.get(cand) != None):
                count[cand] += 1;
            else:
                count[cand] = 1;
    for k in count:
        if (count[k] >= (support/num_partitions)):
            res.append((k,1))
    return res


def counter(buckets,l,k,support,num_partitions):  # x : data in patition;
    freq_set = dict()
    res = []
    count = {}

    l1 = list(set(list(itertools.chain.from_iterable(l)))) #unique elements list

    freq_cand_set = set((list(combinations(l1, k)))) #all the combinations
    #print(len(freq_cand_set))

    if (k>2):
        # print(l)
        for cand in list(freq_cand_set):
            temp = list(combinations(list(cand), k-1))
            for t in temp:
                # print(sorted(tuple(t)))
                if tuple(sorted(t)) not in l:
                    freq_cand_set.remove(cand) #removing the ones without subsets
                    break
    #print(len(freq_cand_set))

    for buc in buckets:
        for tup in freq_cand_set:
            cand = set()
            for t in tup:
                cand.add(t)
            cand = frozenset(cand)
            if cand.issubset(buc):
                if (cand) in freq_set:
                    if (freq_set[cand] < (support/num_partitions)):
                        freq_set[cand] += 1
                        if (freq_set[cand] >= support/num_partitions):
                            yield (tuple(cand), 1)
                else:
                    freq_set[cand] = 1
                    if (freq_set[cand] >= support/num_partitions):
                        yield (tuple(cand), 1)


def freq(buckets,freq_cand_set):
    freq_set = dict()
    res = []

    for buc in buckets:
        for cand in freq_cand_set:

            if (type(cand) == str):
                temp = set()
                temp.add(cand)
                cand = temp
            else:
                cand = set(cand)
            if cand.issubset(buc):
                if frozenset(cand) in freq_set:
                    freq_set[frozenset(cand)] += 1
                else:
                    freq_set[frozenset(cand)] = 1
    for k in freq_set:
        res.append((tuple(k),freq_set[k]))
    return res

read_data = read_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

# Splitting, removing duplicates and constructing baskets
read_data = read_data.map(lambda x : x.split(',')).map(lambda x: (x[0],x[1])).groupByKey(4).map(lambda x : (x[0],set(x[1])))
read_data = read_data.filter(lambda x : len(x[1]) >= k1 ).map(lambda x : set(x[1]))

# counts the data in partions and outputs the candidates
num_partitions = read_data.getNumPartitions()


# singeleton candidates generation
freq_cand_list = read_data.map(lambda x : list(x)).mapPartitions(lambda x : counter_singleton(x,support,num_partitions)).groupByKey().sortBy(lambda x : x[0]).map(lambda x : tuple(x)[0])
freq_cand_list = freq_cand_list.collect()

# print(freq_cand_list)
#
freq_set = []
freq_cands = []
freq_cands.append(sorted(freq_cand_list))


k = 2
while (len(freq_cand_list)!=0):
    #print("while loop: ",  k)
    # freq cands to freq
    freq_set_temp = read_data.mapPartitions(lambda x : freq(x,freq_cand_list)).reduceByKey(add).filter(lambda x : (x[1] >= support)).map(lambda x : tuple(sorted(x[0]))).sortBy(lambda x : x)
    freq_set_temp = freq_set_temp.collect()

    if (len(freq_set_temp) != 0):
        freq_set.append(freq_set_temp)
    else:
        break;

    freq_cand_list = read_data.mapPartitions(lambda x : counter(x,freq_set_temp,k,support,num_partitions)).groupByKey().map(lambda x :tuple(sorted(x[0]))).sortBy(lambda x : x).collect()

    if (len(freq_cand_list) != 0):
        freq_cands.append(freq_cand_list)

    k = k + 1

    end_time = time.time()

    print("Duration: ", k ," : " ,end_time - start_time)



#print(freq_cands)
#print()
#print(freq_set)
outfile = open(output_file,"w")

outfile.write("Candidates:" + "\n")


i = 1;
for cands in freq_cands:
    line = ""
    if (i == 1):
        for cand in cands:
            line = line + '(\''+cand+'\''+')'+','
    else:
        for cand in cands:
            line = line + str(cand)+','
    i = 0
    outfile.write(line[:-1]+"\n")

outfile.write("\n")
outfile.write("Frequent Itemsets:" + "\n")
i = 1;

for cands in freq_set:
    line = ""
    if (i == 1):
        for cand in cands:
            line = line + '(\''+list(cand)[0]+'\''+')'+','
    else:
        for cand in cands:
            line = line + str(tuple(cand))+','
    i = 0
    outfile.write(line[:-1]+"\n")
end_time = time.time()
print("Duration: ", k ," : " ,end_time - start_time)
