# -*- coding: utf-8 -*-

import os
from pyspark import SparkContext
import json,re
import sys
import time
import binascii
import random
import csv
from statistics import median
from pyspark.streaming import StreamingContext
start_time = time.time()
sc = SparkContext("local[*]",'test')
sc.setLogLevel("OFF")
ssc = StreamingContext(sc, 5)

outfile = sys.argv[2]
csv_file = open(outfile, 'w')
csv_write = csv.writer(csv_file, delimiter=',', lineterminator='\n')
csv_write.writerow(["Time","Ground Truth", "Estimation"])
csv_file.close()
#java -cp "$ASNLIB/publicdata/generate_stream.jar" StreamSimulation "$ASNLIB/publicdata/business.json" 9999 100                            


def get_hash(k=25):
    
    hashes = []
    # Define hash function here
    
    #group 1
    a = [random.randrange(1, 100 , 2) for i in range(k)]
    b = [random.randrange(1, 100 , 2) for i in range(k)]
    m = 1024
    p = 1031
    hash_funcs =[[int(a[i]),int(b[i]),p,m] for i in list(range(0, k))]
    hashes = hashes + hash_funcs
    
    # group 2
    a = [random.randrange(1, 100 , 2) for i in range(k)]
    b = [random.randrange(1, 100 , 2) for i in range(k)]
    m = 256#32768
    p = 257#32771
    hash_funcs =[[int(a[i]),int(b[i]),p,m] for i in list(range(0, k))]
    hashes = hashes + hash_funcs
    
    return hashes
    


def get_TZ(x): 
    count = 0
    if (x == 0):
        return 0
    while ((x & 1) == 0): 
        x = x >> 1
        count += 1
       
    return count 


#[[int(a[i]),int(b[i]),p,m] for i in list(range(0, k))]

def hash_val(x,hashes):
    
    return [ get_TZ(((h[0]*x + h[1])%h[2])%h[3]) for h in hashes]
    
    
    
     
   
hashes = get_hash(k=25)

def flm_alg(time,data):
    
    global hashes
    
    data = data.collect() 
    
    grnd_trth = set()
    data_dict = {}
    
    
    groups = 5
    
    for i,d in enumerate(data):
        
        json_dec = json.loads(d)
        city = json_dec["city"]
        grnd_trth.add(city)
        enc = int(binascii.hexlify(city.encode('utf8')),16)        
        data_dict[i] = hash_val(enc,hashes)
        
    
    estimates = []
    
    for h in range(len(hashes)):
        
        max_val = -1
        
        for key in data_dict:
            
            curr_val = data_dict[key][h] 
            
            if (curr_val > max_val):
                max_val = curr_val
               
        estimates.append(2**max_val)
    
    print(estimates)
    grp_avgs = [sum(estimates[i:i+5])/5 for i in range(0,len(estimates),5)]
    
    final_estimate = median(grp_avgs)
    
    
    csv_file = open(outfile, 'a+')
    csv_write = csv.writer(csv_file, delimiter=',', lineterminator='\n')
    csv_write.writerow([str(time),str(len(grnd_trth)), str(final_estimate)])
    csv_file.close()
    
    
        
        
    print(time)
    print(final_estimate)
    print(len(grnd_trth))
    
            
        
        
         
    
    
    
    

    

if __name__ == "__main__":
    start_time = time.time()
    
    grnd_truth = set()

    port_num = int(sys.argv[1])
    
    
    #read_data = sc.textFile(input_file).map(json.loads)
    
    
    read_stream = ssc.socketTextStream("localhost", port_num).window(30,10)
    
    
    read_stream.foreachRDD(flm_alg)
    
    
    ssc.start()             
    ssc.awaitTermination()
    
 
    
    
    
    
    
    
    
    
    
    #read_data = read_data.map(json.loads).map(lambda x : x['city']).filter(lambda x : x)
                                        
    #cities = set(read_data.collect())   
    
    #ead_data.map(lambda x : int(binascii.hexlify(x.encode('utf8')),16)).flatMap(lambda x : hash_val(x,hashes))\
     #                                         .map(lambda x : x[0],x[1]).reduceByKeyAndWindow(max,30,10).map(lambda x : (x[0][1],[x[1]]))\
      #                                        .reduceByKeyAndWindow(lambda x,y : x+y ,30,10).map(lambda x : median(x[1]))
       #                                       .
                                                                        
    #print(len(hashes))
    print()
    
    
    
   
