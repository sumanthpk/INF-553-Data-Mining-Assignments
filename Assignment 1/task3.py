import re
import sys
import json
from pyspark import SparkContext
import time
from itertools import groupby

input_file = sys.argv[1]
output_file = sys.argv[2]
custom = sys.argv[3]
part = int(sys.argv[4])
n= int(sys.argv[5])




start = time.time()

if (custom == "default"):
    sc = SparkContext("local[*]",'test')
    read_data = sc.textFile(input_file)
    rdd=read_data.map(json.loads).map(lambda x: (x["business_id"],1))
    rdd = rdd.reduceByKey(lambda a,b: a+b).filter(lambda x : x[1]>n)
    result = rdd.collect()
    print("default")
    num_part = rdd.glom().count()
    num_elem = rdd.glom().map(lambda x: len(x)).collect()
    output = {"n_partitions" : num_part, "n_items": num_elem   ,"result" : result}
    print(output)
    with open(output_file, 'w') as outfile:
        json.dump(output, outfile)

else:
    sc = SparkContext()
    read_data = sc.textFile(input_file)
    rdd=read_data.map(json.loads).map(lambda x: (x["business_id"],1)).partitionBy(part,lambda x: hash(x))
    rdd = rdd.reduceByKey(lambda a,b: a+b).filter(lambda x : x[1]>n)
    # rdd = rdd.mapPartitions(func).filter(lambda x : x[1]>n)
    result = rdd.collect()
    num_part = part
    num_elem = rdd.glom().map(lambda x: len(x)).collect()
    output = {"n_partitions" : num_part, "n_items": num_elem  , "result" : result}
    with open(output_file, 'w') as outfile:
        json.dump(output, outfile)

end = time.time()

print(end - start)
