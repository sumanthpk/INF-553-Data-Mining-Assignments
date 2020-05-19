import os
from pyspark import SparkContext
import json,re
import sys
import csv


sc = SparkContext("local[*]",'test')
sc.setLogLevel("OFF")
review_file = sys.argv[1]
review_data = sc.textFile(review_file).map(json.loads)
review_data = review_data.map(lambda x : (x['business_id'],x['user_id']))

bus_file = sys.argv[2]
bus_data = sc.textFile(bus_file).map(json.loads)
bus_data = bus_data.filter(lambda x: (x['state'] == 'NV')).map(lambda x: (x['business_id'],1))

# print(review_data.collect())
# print()
# print(bus_data.collect())

data = bus_data.join(review_data).map(lambda x : (x[1][1],x[0])).collect()

final_list = ('user_id','business_id')
data.insert(0,final_list)


csvfile=open('data.csv','w', newline='')
obj=csv.writer(csvfile)
for row in data:
    obj.writerow(row)

csvfile.close()
