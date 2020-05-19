import os
import pyspark
from pyspark import SparkContext
import sys
import time
import random
import math
import csv
import json

Conf = pyspark.SparkConf().setAppName('task1').setMaster('local[*]')
sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'

comp_count = 0


def l2_dist(x1, x2):
    return math.sqrt(sum([(x2[dim] - val)**2 for dim,val in enumerate(x1)]))



def computing_mean(x,y):
    count = x[1]+y[1]
    D = len(x[0])-1
    sumq = [0] *(D+1)
    for d in range(1, D + 1):
        sumq[d] = float(x[0][d])+ float(y[0][d])
    return  (sumq,count)




def kmeans_utils(x, clstr_cent):

    min_dist = -1
    clst_pnt = None

    for clst_key in clstr_cent:
        try:
            dist = l2_dist(x["pnt"], clstr_cent[clst_key])
        except:
            print(clstr_cent.keys())
        #dist = 10000
        if (min_dist == -1 or dist < min_dist):
            min_dist = dist
            clst_pnt = clst_key

    return (clst_pnt,x)

#
# def compute_mean(x):
#     [for pnt in zip(*x)]


def K_MEANS(clstr_cent, read_data, it):
    # clstr_cent --> list of tuples. Each tuple is data point from the file. Elements in tuple are of string. Each tuple of lenght D.
    # (pnt_ind, d1 , d2, ....)
    # read_data --> RDD, with each row list of data points
    # This is also list of tuple in the same format as above

    itert = 0
    while (itert < it):

        pnt_clstr_map = read_data.map(lambda x: kmeans_utils(x, clstr_cent)).mapValues(lambda x : x["pnt"]).groupByKey()

        clstr_cent = pnt_clstr_map.map(lambda x : (x[0],list(x[1]))).mapValues(lambda x : [sum(i)/len(i) for i in zip(*x)]).collectAsMap()

        #.map( lambda x : x[0],([x[1]["values"] for pnt in x[1]])

        #     .reduceByKey(lambda x,y : computing_mean(x,y)).map(lambda x : x[1])\
        #                                                             .map(lambda x : tuple([float(i)/x[1] for i in x[0]])).collect()
        itert = itert + 1

    return clstr_cent,pnt_clstr_map  # RDD. Each ROW is a list of all tuples in one cluster


def find_statistics(x):

    N = len(x)
    sumq = [sum(i) for i in zip(*x)]
    sum_sq = [sum([i*i for i in y]) for y in zip(*x)]

    return {'sum': sumq , 'sum_sq' : sum_sq ,'N':N}


def mahlnobis(pnt, avg, var):
    try:
        mah =  math.sqrt(sum([((pnt[i] - avg[i])/var[i])**2 for i in range(len(pnt))]))
    except:
        mah = math.sqrt(sum([((pnt[i] - avg[i])) ** 2 for i in range(len(pnt))]))
    return mah

def check_discard(x,clust_params):

    alpha = 3
    D = len(x[1][0])
    alpha_th = alpha * math.sqrt(D)

    avg = ([(i / clust_params[x[0]]['N']) for i in clust_params[x[0]]['sum']])
    msum_sq = ([(i / clust_params[x[0]]['N']) for i in clust_params[x[0]]['sum_sq']])

    var = [math.sqrt(i[0]-i[1]**2) for i in zip(msum_sq,avg)]

    res = [((x[0],pnt)if mahlnobis(pnt,avg,var) < alpha_th else (-1,pnt)) for pnt in x[1]]

    return res




def map_discard(x,clstrs):

    alpha = 3
    D = len(x['pnt'])
    alpha_th = alpha * math.sqrt(D)

    for clst_key in clstrs:

        avg = ([(i / clstrs[clst_key]['N']) for i in clstrs[clst_key]['sum']])
        msum_sq = ([(i / clstrs[clst_key]['N']) for i in clstrs[clst_key]['sum_sq']])
        var = [math.sqrt(i[0] - i[1] ** 2) for i in zip(msum_sq, avg)]

        if (mahlnobis(x['pnt'],avg,var) < alpha_th):
            return (clst_key,x)

    return (-1,x)

def update_cluster(x,d_set):

    x[1]['N'] += d_set[x[0]]['N']
    for i,val in enumerate(x[1]['sum']):
        x[1]['sum'][i]  += d_set[x[0]]['sum'][i]
        x[1]['sum_sq'][i] += d_set[x[0]]['sum_sq'][i]

    return x

def cluster_points(rmng_pnts, n_cluster):
    global comp_count
    rnd_num = set()
    rmng_pnts = list(rmng_pnts)
    while (len(rnd_num) != n_cluster):
        rnd_num = set(random.sample(range(0, len(rmng_pnts), 1), n_cluster))
    clstr_cent = set([rmng_pnts[i]["ind"] for i in rnd_num])
    # clstr_cent = {}

    # print(clstr_cent)

    # print(clstr_cent)

    rmng_rdd = sc.parallelize(rmng_pnts)
    clstr_cent = rmng_rdd.filter(lambda x : x['ind'] in clstr_cent).map(lambda x : x["pnt"])\
                                            .zipWithIndex().map(lambda x : (x[1]+comp_count,x[0])).collectAsMap()

    comp_count += len(clstr_cent)

    #print(list(clstr_cent.keys()))

    clstrs, clst_pnt = K_MEANS(clstr_cent, rmng_rdd, 5)

    # clst_pnt = rmng_rdd.map(lambda x: kmeans_utils(x, clstrs))
    #
    # clust_params = clst_pnt.mapValues(lambda x: x["pnt"]).groupByKey().mapValues(
    #     lambda x: find_statistics(list(x))) \
    #     .collectAsMap()

    clust_params = clst_pnt.mapValues(lambda x: find_statistics(list(x))).collectAsMap()
    pnt_clst_rdd = clst_pnt.mapValues(list).flatMap(lambda x: check_discard(x, clust_params))

    c_set = pnt_clst_rdd.filter(lambda x: x[0] != -1).groupByKey().mapValues(lambda x: find_statistics(list(x))) \
        .collectAsMap()

    r_set = pnt_clst_rdd.filter(lambda x: x[0] == -1).map(lambda x: x[1])

    return c_set,r_set


def merge_clusters(c_set, d_set):

    alpha = 3
    c_set_temp = {}

    merged = 0


    for key in c_set:

        avg_c = ([(i / c_set[key]['N']) for i in c_set[key]['sum']])
        flag = 0
        for clst in d_set:
            avg = ([(i / d_set[clst]['N']) for i in d_set[clst]['sum']])
            msum_sq = ([(i / d_set[clst]['N']) for i in d_set[clst]['sum_sq']])
            var = [math.sqrt(i[0] - i[1] ** 2) for i in zip(msum_sq, avg)]
            D = len(avg)
            if (mahlnobis(avg_c, avg, var) < alpha*math.sqrt(D)):
                #print("merged cluster")
                merged += 1
                d_set[clst]['N'] += c_set[key]['N']

                for i, val in enumerate(c_set[key]['sum']):
                    d_set[clst]['sum'][i] += c_set[key]['sum'][i]
                    d_set[clst]['sum_sq'][i] += c_set[key]['sum_sq'][i]
                flag = 1
                break
        if (flag == 0):
            c_set_temp[key] = c_set[key]

    return d_set,c_set_temp

def cnt_pnts(d_set):
    tot_pnts = 0
    if (len(d_set) == 0):
        return 0
    for each in d_set:
        tot_pnts = tot_pnts + d_set[each]['N']
    return tot_pnts


if __name__ == "__main__":
    start_time = time.time()
    path = sys.argv[1]
    n_cluster = int(sys.argv[2])
    out_file1 = sys.argv[3]
    out_file2 = sys.argv[4]
    data_files = os.listdir(path)
    first_file = True
    file_count = 1


    csv_file = open(out_file2, 'w')
    csvwriter = csv.writer(csv_file)
    csvwriter.writerow(
        ['round_id', "nof_cluster_discard", "nof_point_discard", "nof_cluster_compression", "nof_point_compression",
         'nof_point_retained'])

    for file in sorted(data_files):
        file_path = os.path.join(path, file)

        if (first_file):

            first_file = False
            read_data = sc.textFile(file_path).map(lambda x: list(x.rstrip().split(','))).\
                                               map(lambda x : {"ind" : int(x[0]), "pnt" : [float(i) for i in x[1:]]})


            pnt_ind = read_data.map(lambda x : (tuple(x["pnt"]),x["ind"])).collectAsMap()


            num_sam = read_data.count()

            rnd_num = set()

            while (len(rnd_num) != 3 * n_cluster):
                rnd_num = set(random.sample(range(0, num_sam, 1), 3 * n_cluster))
            clstr_cent = read_data.filter(lambda x: x["ind"] in rnd_num).map(lambda x: (x["ind"], x["pnt"])).collectAsMap()

            clstrs = K_MEANS(clstr_cent, read_data, 3)

            try:
                outliers = set(read_data.map(lambda x: kmeans_utils(x, clstrs)).groupByKey().filter(lambda x : len(x[1]) == 1)\
                                                                                            .map(lambda x : x[0]).collect())
            except:
                outliers = set()

            print(outliers)
            break


            rnd_num = set()
            while (len(rnd_num) !=  n_cluster or len(rnd_num.intersection(outliers)) != 0):
                rnd_num = set(random.sample(range(0, num_sam, 1), n_cluster))
            clstr_cent = read_data.filter(lambda x: x["ind"] in rnd_num).map(lambda x : x["pnt"])\
                                            .zipWithIndex().map(lambda x : (x[1],x[0])).collectAsMap()


            clstrs,clst_pnt = K_MEANS(clstr_cent, read_data, 30)
           # clst_pnt = read_data.map(lambda x : kmeans_utils(x,clstrs))\

           # clust_params = clst_pnt.mapValues(lambda x : x["pnt"]).groupByKey().mapValues(lambda  x : find_statistics(list(x)))\
           #                                                                .collectAsMap()

            clust_params = clst_pnt.mapValues(lambda  x : find_statistics(list(x))).collectAsMap()

            pnt_clst_rdd = clst_pnt.mapValues(list).flatMap(lambda x : check_discard(x,clust_params))

            d_set =  pnt_clst_rdd.filter(lambda x : x[0]!=-1).groupByKey().mapValues(lambda  x : find_statistics(list(x)))\
                                                                           .collectAsMap()
            rmng_pnts=  pnt_clst_rdd.filter(lambda x : x[0]==-1).map(lambda x : x[1]).map(lambda x : {"ind" : pnt_ind[tuple(x)],"pnt" : x}).collect()


            print(len(rmng_pnts))

            if (len(rmng_pnts) < 2 * n_cluster):
                c_set = {}
                r_set = rmng_pnts
            else:
                c_set, r_set = cluster_points(rmng_pnts, n_cluster)
                r_set = r_set.map(lambda x : {"ind" : pnt_ind[tuple(x)],"pnt" : x}).collect()

            end_time = time.time()
            print(end_time - start_time)



            #print(len(clust_params))

        else:
            read_data = sc.textFile(file_path).map(lambda x: list(x.rstrip().split(','))).\
                                               map(lambda x : {"ind" : int(x[0]), "pnt" : [float(i) for i in x[1:]]})

            pnt_ind = read_data.map(lambda x: (tuple(x["pnt"]), x["ind"])).collectAsMap()
            # print(read_data.count())
            pnt_clstr = read_data.map(lambda x : map_discard(x,d_set))
            d_set_temp = pnt_clstr.filter(lambda x :x[0] != -1).mapValues(lambda x : x["pnt"]).groupByKey() \
                                                                                        .mapValues(lambda x: find_statistics(list(x)))\
                                                                                        .map(lambda x : update_cluster(x,d_set))\
                                                                                        .collectAsMap()

            for key in d_set_temp:
                d_set[key] = d_set_temp[key]

            rmng_pnts = pnt_clstr.filter(lambda x: x[0] == -1).map(lambda x : x[1]).collect()
            if (len(rmng_pnts) < 2 * n_cluster):
                c_set_temp = {}
                r_set = rmng_pnts + r_set
            else:
                c_set_temp, r_set_temp= cluster_points(rmng_pnts, 2*n_cluster)

                r_set_temp = r_set_temp.map(lambda x: {"ind": pnt_ind[tuple(x)], "pnt": x}).collect()
                r_set += r_set_temp

                if (len(c_set) != 0):
                    c_set,c_set_temp = merge_clusters(c_set_temp, c_set)

                c_set.update(c_set_temp)
                d_set,c_set = merge_clusters(c_set,d_set)


            end_time = time.time()
            print(end_time - start_time)

        csvwriter.writerow([file_count, len(d_set), cnt_pnts(d_set), len(c_set), cnt_pnts(c_set), len(r_set)])
        print(file_count, len(d_set), cnt_pnts(d_set), len(c_set), cnt_pnts(c_set), len(r_set))
        file_count = file_count + 1

    result = {}

    for file in data_files:
        file_path = os.path.join(path, file)
        read_data = sc.textFile(file_path).map(lambda x: list(x.rstrip().split(','))). \
            map(lambda x: {"ind": int(x[0]), "pnt": [float(i) for i in x[1:]]}).map(lambda x : map_discard(x,d_set))\
                                                                                .mapValues(lambda x: x["ind"])\
                                                                                .map(lambda x : (str(x[1]),x[0])).collectAsMap()
        result.update(read_data)


    with open(out_file1, 'w') as fp:
        json.dump(result, fp)

    end_time = time.time()
    print(end_time - start_time)


