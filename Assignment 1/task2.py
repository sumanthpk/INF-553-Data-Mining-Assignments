import re
import sys
import json
from collections import  OrderedDict
from pyspark import SparkContext

reviews_path = 'review.json'#sys.argv[1]


categories = []
filename = sys.argv[1]
bus_file = sys.argv[2]
output_file = sys.argv[3]
n = int(sys.argv[5])
spark = sys.argv[4]


if (spark == "no_spark"):
    busi_dict = {}
    cate_dict = {}

    with open(bus_file) as f2:
        for line2 in f2:
            data2 = json.loads(line2)
            cate = data2['categories']
            if (cate is not None):
                categories = cate.split(",")
                for i in range(len(categories)):
                    categories[i] = categories[i].strip()
                busi_dict[data2['business_id']] = categories;

    with open(filename) as f:
        for line in f:
            data = json.loads(line)
            busi_id = data['business_id']

            if busi_id in busi_dict:
                categories = busi_dict[busi_id]

                for category in categories:
                    if (category in cate_dict):
                        cate_dict[category][0] = cate_dict[category][0] + data['stars']
                        cate_dict[category][1] = cate_dict[category][1] + 1
                    else:
                        cate_dict[category] = [data['stars'],1]

    dd = OrderedDict(sorted(cate_dict.items(), key=lambda x: (-x[1][0]/x[1][1],x[0])))
    n = n if len(dd) > n else len(dd)

    categories = []
    for k in dd:
        categories.append((k,dd[k][0]/dd[k][1]))
        n = n-1
        if (n <= 0):
            break
else:
    sc = SparkContext("local[*]",'test')
    rev_data = sc.textFile(filename)
    bus_data = sc.textFile(bus_file)

    rev_rdd = rev_data.map(json.loads)
    rating_rdd = rev_rdd.map(lambda x: (x["business_id"] , ([x["stars"],1])))
    rating_rdd = rating_rdd.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))

    bus_rdd = bus_data.map(json.loads).filter (lambda x: x['categories'] is not None)
    def func(x):
        for i in range(len(x[1])):
            x[1][i] = x[1][i].strip()

        return [x[0],x[1]]

    def func2(x):
        res = []
        for i in range(len(x[1][1])):
            res.append((x[1][1][i],x[1][0]))
        return res

    cate_rdd = bus_rdd.map(lambda x: [x["business_id"],x["categories"].split(",")]).map(func)
    final_rdd = rating_rdd.join(cate_rdd).flatMap(func2).reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    final_rdd = final_rdd.map(lambda x: (x[0],x[1][0]/x[1][1])).sortBy(lambda x : (-x[1],x[0]))
    categories = final_rdd.take(n)

output = {"result" : []}

for c in categories:
    output["result"].append([c[0],c[1]])

with open(output_file, 'w') as outfile:
    json.dump(output, outfile)
