import os
from pyspark import SparkContext
import json,re
import sys


sc = SparkContext("local[*]",'test')
sc.setLogLevel("OFF")
input_file = sys.argv[1]
read_data = sc.textFile(input_file)
rdd=read_data.map(json.loads)


cnt = rdd.count()


y= sys.argv[4]
def func(x):
    date = x['date']
    if date[0:4] == y :
        return 1
    else:
        return 0;
year = rdd.filter(func).count()


users_c = rdd.map(lambda x: x['user_id']).distinct().count()

m = int(sys.argv[5])
users = rdd.map(lambda x : (x['user_id'],1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: (-a[1],a[0])).map(lambda a: [a[0],a[1]])
lists = users.take(m)

n = int(sys.argv[6])
stopwords_path = sys.argv[3]
stopwords = sc.textFile('stopwords')
words_set = set(stopwords.flatMap(lambda x: x.splitlines()).collect())
words_rdd = rdd.map(lambda x: x['text'])
words_rdd = words_rdd.map(lambda x : re.sub(r'[^\w\s]','',x)).map(lambda x : x.split(' '))

def func (w):
    res = []
    for word in w:
        temp = word.lower()
        if (temp not in words_set and temp.isnumeric() == False and temp != ''):
            res.append(temp)
    return res

words_rdd = words_rdd.flatMap(func).map(lambda x : (x,1)).reduceByKey(lambda a,b: a+b).sortBy(lambda a: -(a[1],a[0])).map(lambda x: x[0])
words = words_rdd.take(n)

output = {}
output_file = sys.argv[2]
output = {"A" : cnt, "B" : year, "C" : users_c, "D" : lists, "E": words}

with open(output_file, 'w') as outfile:
    json.dump(output, outfile)
