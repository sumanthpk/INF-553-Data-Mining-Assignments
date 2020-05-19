import os
import pyspark
from pyspark import SparkContext
from operator import itemgetter, attrgetter
from collections import deque
from itertools import combinations
# fron
import sys
import time

Conf = pyspark.SparkConf().setAppName('task2').setMaster('local[3]')
sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")


def graph(x, user_bus, th):
    set_1 = set(user_bus[x[0]])
    set_2 = set(user_bus[x[1]])
    bus_count = len(set_1.intersection(set_2))

    # print(set_1, set_2, bus_count)

    if (int(bus_count) >= th):
        return (x, 1)
    else:
        return (x, 0)


def bfs(x, graph_adj):
    node_visited = {}
    q = deque()
    q.append(x)
    q.append(-1)
    child_dict = {}
    nodes_order = []

    node_visited[x] = 1
    temp_dict = {}

    while q:
        s = q.popleft()

        if (s == -1):
            if (len(temp_dict.keys()) != 0):
                for key in list(temp_dict.keys()):
                    q.append(key)
                    node_visited[key] = temp_dict[key]
                q.append(-1)
                temp_dict = {}

        else:
            nodes_order = [s] + nodes_order
            for i in list(graph_adj[s]):
                if i not in node_visited:
                    # q.append(i)
                    if s not in child_dict:
                        child_dict[s] = {i}
                    else:
                        child_dict[s].add(i)

                    if i not in temp_dict:
                        temp_dict[i] = node_visited[s]
                    else:
                        temp_dict[i] = temp_dict[i] + node_visited[s]

    # parent_dict = {}
    node_scores = {}
    res = []

    # for k,v in child_dict:
    #     for ele in v:
    #         if ele not in parent_dict:
    #             parent_dict[ele] = {k}
    #         else:
    #             parent_dict[ele].add(k)k

    for node in nodes_order:
        if node not in child_dict:  # LEAF NODE
            node_scores[node] = 1
        else:
            score = 1
            for child in child_dict[node]:
                try:
                    temp = (  node_scores[child] * node_visited[node] / node_visited[child])
                    score = score + temp
                    res.append((tuple(sorted((child, node))), temp))
                except:
                    print("Key Error: ", node, child)
            node_scores[node] = score
    return res

def communities(graph_adj):
    nodes_visted = set()
    communities = []
    stack = []

    for node in list(graph_adj.keys()):

        if node not in nodes_visted:

            stack.append(node)
            community = []
            comm_set= set()
            comm_set.add(node)
            while (len(stack)):

                ele = stack[-1]
                stack.pop()
                for node1 in list(graph_adj[ele]):
                    if (node1 not in comm_set): # adding the connected nodes to the stack
                        stack.append(node1)
                        comm_set.add(node1)
            communities.append(list(comm_set))
            nodes_visted = nodes_visted.union(comm_set)

    return communities

def modularity(communities,adj_mat,m):

    mod = 0
    for comm in communities:

        for pair in list(combinations(comm,2)):

            ki =  len(adj_mat[pair[0]])
            kj =  len(adj_mat[pair[1]])

            if pair[1] in adj_mat[pair[0]]:
                Aij = 1
            else:
                Aij = 0

            mod = mod + (Aij - (ki*kj)/(2*m))

    return mod/(2*m)


def user_ids(x, user_nam):
    temp = tuple(sorted([user_nam[x[0][0]], user_nam[x[0][1]]]))
    return (temp, x[1])


if __name__ == "__main__":
    start_time = time.time()
    threshold = int(sys.argv[1])
    input_file = sys.argv[2]
    output_file_bet = sys.argv[3]
    output_file_comm = sys.argv[4]

    read_data = sc.textFile(input_file)
    header = read_data.first()

    # user-id, business-id
    read_data = read_data.filter(lambda line: line != header).map(lambda x: x.split(',')).map(lambda x: (x[0], x[1]))

    user_ind = read_data.map(lambda x: (x[0], 1)).groupByKey().map(lambda x: (x[0])).zipWithIndex().collectAsMap()
    bus_ind = read_data.map(lambda x: (x[1], 1)).groupByKey().map(lambda x: (x[0])).zipWithIndex().collectAsMap()

    user_bus_rdd = read_data.groupByKey().map(lambda x: (x[0], list(x[1])))
    user_bus_rdd = read_data.map(lambda x: (user_ind[x[0]], bus_ind[x[1]])).groupByKey().map(
        lambda x: (x[0], list(x[1])))

    user_bus = user_bus_rdd.collectAsMap()

    user_bus_rdd = user_bus_rdd.map(lambda x: x[0])
    user_graph = user_bus_rdd.cartesian(user_bus_rdd).filter(lambda x: x[0] != x[1]) \
        .map(lambda x: graph(x, user_bus, threshold)) \
        .filter(lambda x: x[1] == 1) \
        .map(lambda x: x[0]) \
        .flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).groupByKey().map(lambda x: (x[0], set(x[1]))).sortByKey()

    graph_adj = user_graph.collectAsMap()

    user_nam = {v: k for k, v in user_ind.items()}

    graph_bet = user_graph.map(lambda x: x[0]).flatMap(lambda x: bfs(x, graph_adj)).reduceByKey(
        lambda x, y: x + y).map(lambda x: (x[0], x[1] / 2)) \
        .map(lambda x: user_ids(x, user_nam)) \
        .collect()


    graph_bet = sorted(graph_bet, key=lambda x: (-x[1], x[0][0]))

    outfile = open(output_file_bet, "w")
    for pair in graph_bet:
        line = str(tuple(pair[0])) + "," + str(pair[1])
        outfile.write(line + "\n")
    outfile.close()

    temp_adj = graph_adj
    m = len(graph_bet)

    final_com = communities(temp_adj)
    max_mod = modularity(final_com, graph_adj, m)
    edge_rem = (user_ind[graph_bet[0][0][0]], user_ind[graph_bet[0][0][1]])

    while(len(graph_bet) != 0):

        temp_adj[edge_rem[0]].remove(edge_rem[1])
        temp_adj[edge_rem[1]].remove(edge_rem[0])

        comm = communities(temp_adj)
        mod = modularity(comm, graph_adj,m)

        if (mod > max_mod):
            max_mod = mod
            final_com = comm

        print(len(graph_bet)-1, len(comm), mod)

        graph_bet = user_graph.map(lambda x: x[0]).flatMap(lambda x: bfs(x, temp_adj)).reduceByKey(
            lambda x, y: x + y).map(lambda x: (x[0], x[1] / 2)).collect()

        graph_bet = sorted(graph_bet, key=lambda x: (-x[1]))



        if(len(graph_bet)):
            edge_rem = (graph_bet[0][0][0], graph_bet[0][0][1])


    final_com = sorted(final_com, key = lambda x : len(x))

    for i in range(len(final_com)):
        for j in range(len(final_com[i])):
            final_com[i][j] = user_nam[final_com[i][j]]

        final_com[i] = sorted(final_com[i])

    print(max_mod)
    print(len(final_com))

    outfile = open(output_file_comm, "w")
    for comm in final_com:
        line = ""
        for node in comm:
            line = line + "\'" + node + "\'"+ ", "
        outfile.write(line[:-2] + "\n")
    outfile.close()


    end_time = time.time()
    print(end_time - start_time)

