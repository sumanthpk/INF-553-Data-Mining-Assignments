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

sc = SparkContext("local[*]",'test')
input_file = sys.argv[3]
case = int(sys.argv[1])
read_data = sc.textFile(input_file)
support = int(sys.argv[2])
output_file = sys.argv[4]
start_time = time.time()

def counter_singleton(x,support,num_partitions):  # x : data in patition;
    count = {}
    res = []
    cands = list(x)
    for cand in cands:
        if (count.get(cand) != None):
            count[cand] += 1;
        else:
            count[cand] = 1;
    for k in count:
        if (count[k] >= (support/num_partitions)):
            res.append((k,1))
    return res


def counter(x,l,k,support,num_partitions):  # x : data in patition;
    freq_set = dict()
    res = []
    buckets = list(x)#data in a partition
    count = {}

    l1 = list(set(list(itertools.chain.from_iterable(l)))) #unique elements list
    freq_cand_set = set((list(combinations(l1, k)))) #all the combinations
    if (k>2):
        for cand in list(freq_cand_set):
            temp = list(combinations(list(cand), k-1))
            for t in temp:
                if set(t) not in l:
                    freq_cand_set.remove(cand) #removing the ones without subsets
                    break
    final = set()
    for tup in freq_cand_set:
        temp = set()
        for t in tup:
            temp.add(t)
        final.add(frozenset(temp))

    freq_cand_set = final

    for buc in buckets:
        for cand in freq_cand_set:
            if (type(cand) == str):
                 cand = {cand}
            if cand.issubset(set(buc[1])):
                if frozenset(cand) in freq_set:
                    freq_set[frozenset(cand)] += 1
                else:
                    freq_set[frozenset(cand)] = 1
    for k in freq_set:
        if (freq_set[k] >= (support/num_partitions)):
            res.append((k,1))
    return res
#
#     for cand in cands:
#         if (count.get(cand) != None):
#             count[cand] += 1;
#         else:
#             count[cand] = 1;
#     for k in count:
#         if (count[k] >= (support/num_partitions)):
#             res.append((k,1))
#     return res



def freq(x,freq_cand_set):
    freq_set = dict()
    res = []
    buckets = list(x)#data in a partition
    for buc in buckets:
        for cand in freq_cand_set:
            if (type(cand) == str):
                 cand = {cand}
            if cand.issubset(set(buc[1])):
                if frozenset(cand) in freq_set:
                    freq_set[frozenset(cand)] += 1
                else:
                    freq_set[frozenset(cand)] = 1
    for k in freq_set:
        res.append((k,freq_set[k]))
    return res


# removing header
if (case == 1):
    read_data = read_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

    # Splitting, removing duplicates and constructing baskets
    read_data = read_data.map(lambda x : x.split(',')).map(lambda x: (x[0],x[1])).distinct().groupByKey()

    # counts the data in partions and outputs the candidates

    num_partitions = read_data.getNumPartitions()

    # singeleton candidates generation
    freq_cand_list = read_data.flatMap(lambda x: x[1]).mapPartitions(lambda x : counter_singleton(x,support,num_partitions)).groupByKey().map(lambda x : x[0]).collect();

    freq_set = []
    freq_cands = []
    freq_cands.append(sorted(set(freq_cand_list)))

    freq_cand_set = set(freq_cand_list)

    k = 2
    while (len(freq_cand_set)!=0):

        # freq cands to freq
        freq_set_temp = read_data.mapPartitions(lambda x : freq(x,freq_cand_set)).reduceByKey(add).filter(lambda x : (x[1] >= support)).map(lambda x: x[0]).collect()

        # Processing and saving the frequent items
        l = []
        l1 = []
        for set1 in freq_set_temp:
            l.append(set(set1))
            l1.append(sorted(tuple(set1)))
        if (len(l) != 0):
            freq_set.append(sorted(l1))

        # print(l)
        freq_cand_set = read_data.mapPartitions(lambda x : counter(x,l,k,support,num_partitions)).groupByKey().map(lambda x : x[0]).collect();

        # Processing and saving the frequent items
        l = []
        for set1 in freq_cand_set:
            l.append(sorted(tuple(set1)))
        if (len(l) != 0):
            freq_cands.append(sorted(l))

        k = k + 1

if (case == 2):
    read_data = read_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

    # Splitting, removing duplicates and constructing baskets
    read_data = read_data.map(lambda x : x.split(',')).map(lambda x: (x[1],x[0])).distinct().groupByKey()

    # counts the data in partions and outputs the candidates

    num_partitions = read_data.getNumPartitions()

    # singeleton candidates generation
    freq_cand_list = read_data.flatMap(lambda x: x[1]).mapPartitions(lambda x : counter_singleton(x,support,num_partitions)).groupByKey().map(lambda x : x[0]).collect();

    freq_set = []
    freq_cands = []
    freq_cands.append(sorted(set(freq_cand_list)))

    freq_cand_set = set(freq_cand_list)

    k = 2
    while (len(freq_cand_set)!=0):

        # freq cands to freq
        freq_set_temp = read_data.mapPartitions(lambda x : freq(x,freq_cand_set)).reduceByKey(add).filter(lambda x : (x[1] >= support)).map(lambda x: x[0]).collect()

        # Processing and saving the frequent items
        l = []
        l1 = []
        for set1 in freq_set_temp:
            l.append(set(set1))
            l1.append(sorted(tuple(set1)))
        if (len(l) != 0):
            freq_set.append(sorted(l1))

        # print(l)
        freq_cand_set = read_data.mapPartitions(lambda x : counter(x,l,k,support,num_partitions)).groupByKey().map(lambda x : x[0]).collect();

        # Processing and saving the frequent items
        l = []
        for set1 in freq_cand_set:
            l.append(sorted(tuple(set1)))
        if (len(l) != 0):
            freq_cands.append(sorted(l))

        k = k + 1




        # frequent cands generation from prev freq items
        # l1 = list(set(list(itertools.chain.from_iterable(l)))) #unique elements list
        # freq_cand_set = set((list(combinations(l1, k)))) #all the combinations
        # if (k>2):
        #     for cand in list(freq_cand_set):
        #         temp = list(combinations(list(cand), k-1))
        #         for t in temp:
        #             if set(t) not in l:
        #                 freq_cand_set.remove(cand) #removing the ones without subsets
        #                 break
        #
        # if (len(freq_cand_set) != 0):
        #     freq_cands.append(freq_cand_set)
        #
        # print(freq_cand_set)
        #
        # # processing the cands
        # final = set()
        # for tup in freq_cand_set:
        #     temp = set()
        #     for t in tup:
        #         temp.add(t)
        #     final.add(frozenset(temp))
        # k = k+1
        # freq_cand_set = final
        # print(final)
        # print()


with open('output_file', 'w') as filehandle:
    filebuffer = ["a line of text", "another line of text", "a third line"]
    filehandle.writelines("%s\n" % line for line in filebuffer)

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
            line = line + str(tuple(cand))+','
    i = 0
    outfile.write(line[:-1]+"\n")

outfile.write("\n")
outfile.write("Frequent Itemsets:" + "\n")
i = 1;
for cands in freq_set:
    line = ""
    if (i == 1):
        for cand in cands:
            line = line + '(\''+cand[0]+'\''+')'+','
    else:
        for cand in cands:
            line = line + str(tuple(cand))+','
    i = 0
    outfile.write(line[:-1]+"\n")






end_time = time.time()

print("Duration: ",end_time - start_time)
# #
# print(freq_cands)
# print()
# print(freq_set)
