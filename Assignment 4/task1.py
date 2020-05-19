import os
import pyspark
from pyspark import SparkContext
from graphframes import *
import json
import sys
import time
from pyspark.sql import Row
from pyspark import SQLContext
from pyspark.sql import functions as F


os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

Conf = pyspark.SparkConf().setAppName('task1').setMaster('local[3]')
sc = SparkContext(conf = Conf)
sc.setLogLevel("ERROR")
sqlContext = SQLContext(sc)


def graph(x,user_bus,th):
    
    set_1 = set(user_bus[x[0]])
    set_2 = set(user_bus[x[1]])
    bus_count = len(set_1.intersection(set_2))
    if (int(bus_count) >= th):
        return (x,1)
    else:
        return (x,0)
    

if __name__ == "__main__":
    start_time = time.time()
    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file = sys.argv[3]
    read_data = sc.textFile(input_file)
    header = read_data.first()
    
    # user-id, business-id
    read_data = read_data.filter(lambda line: line != header).map(lambda x : x.split(','))\
                                                             .map(lambda x : (x[0],x[1]))\
                                                             .groupByKey()\
                                                             .map(lambda x : (x[0],list(x[1])))
    
          
    user_bus = read_data.collectAsMap()
    
    user_bus_rdd = read_data.map(lambda x : (x[0],1)).groupByKey().map(lambda x : x[0])
    user_graph = user_bus_rdd.cartesian(user_bus_rdd).filter(lambda x : x[0]!=x[1])\
                                                        .map(lambda x : graph(x,user_bus,threshold))\
                                                        .filter(lambda x : x[1] == 1)\
                                                        .map(lambda x : x[0])
                
    users = user_graph.flatMap(lambda x : [(x[0],1),(x[1],1)]).groupByKey().map(lambda x : x[0])
    row1 = Row("id")
    row2 = Row("src","dst")
    
    user_edges_df = sqlContext.createDataFrame(user_graph.map(lambda x: row2(*x)))
    user_vertices_df = sqlContext.createDataFrame(users.map(row1))
  
       
    g = GraphFrame(user_vertices_df, user_edges_df)
    result = g.labelPropagation(maxIter=5)
    result = result.groupBy("label").agg(F.collect_list("id"))
 
    result_rdd = result.rdd.map(lambda x : sorted(x[1])).sortBy(lambda x : (len(x),x[0])).collect()
    
    
   # with open(output_file,"w") as outfile:
    #    for com in result_rdd:
     #       string = str(com)
      #      json.dump(string[2:-2], outfile)
       #     outfile.write("\n")
    
    outfile = open(output_file,"w")
    
    for com in result_rdd:
        line = ""
        for user in com:
            line = line + "\'" + str(user) + "\'" ", "
        outfile.write(line[:-2] +"\n")
        
    outfile.close()
    
  
  
  
    end_time = time.time()
    print( end_time - start_time)

