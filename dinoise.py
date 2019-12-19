'''
Copyright (c) 2019, chdavalas
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.'''


import argparse
import subprocess
from os import system
from numpy import zeros
from math import log,sqrt
from time import time,sleep
from operator import add,itemgetter
from random import choice,seed
from collections import Counter
from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from pyspark.serializers import MarshalSerializer
from scipy.optimize import linear_sum_assignment 


# Swap key-value pairs (k,v)->(v,k)
def swap(pair):
    return (pair[1],pair[0])


# Read edge list text file and map lines as key-value pairs 
def line_to_edge(txtline):
    pair = txtline.split(' ')
    return (str(pair[0]),str(pair[1]))


# Transform a shallow copy of a graph edgelist to a deep copy 
def deep_copy(G,PARTS):
    return G.union(G.map(swap)).repartition(PARTS)\
            .persist(StorageLevel.DISK_ONLY)


def set_left_node_as_key(pair):
    return (pair[0][0],(pair[0][1],pair[1]))


def set_right_node_as_key(pair):
    return (pair[1][0],(pair[0],pair[1][1]))


def set_pair_back(pair):
    return ((pair[1][0],pair[0]),pair[1][1])


# Ensure that all pairs with common nodes
# have been cleared based on score
def node_resolution(rdd,PARTS,choose_by_max=True):

    def keep_max(a,b):
        if   a[1]>b[1]: return a
        elif a[1]<b[1]: return b
        else          : return (max(a[0],b[0]),a[1])

    def keep_min(a,b): 
        if   a[1]<b[1]: return a
        elif a[1]>b[1]: return b
        else          : return (max(a[0],b[0]),a[1])

    if choose_by_max==True:
        def choose_by(x,y):return keep_max(x,y)
    else:
        def choose_by(x,y):return keep_min(x,y)

    return rdd.map(set_left_node_as_key)\
              .partitionBy(PARTS)\
              .reduceByKey(choose_by)\
              .map(set_right_node_as_key)\
              .partitionBy(PARTS)\
              .reduceByKey(choose_by)\
              .map(set_pair_back)


# Change pair format
def encode_tuple(tup):
    return str(tup[0])+"<->"+str(tup[1])


# Change pair format
def decode_tuple(edge):
    e=edge.split("<->")
    return (e[0],e[1])


# Manhattan distance
def metric(v1,v2):

    def set_equal_length(v1,v2):
        if len(v1)>len(v2):
            v2+=[0 for x in range(len(v1)-len(v2))]
        elif len(v1)<len(v2):
            v1+=[0 for x in range(len(v2)-len(v1))]

        return v1,v2

    V1,V2 = set_equal_length(v1,v2)
    return sum([abs(i-j) for i,j in zip(V1,V2)])


# SeGen
def seed_generator(sc,G1,G2,seed_num,PARTS,offset=0):

    degG1 = G1.mapValues(lambda x:1).reduceByKey(add)
    degG2 = G2.mapValues(lambda x:1).reduceByKey(add)

    rank_nodesG1 = degG1.sortBy(lambda x:-x[1]).keys().zipWithIndex()
    rank_nodesG2 = degG2.sortBy(lambda x:-x[1]).keys().zipWithIndex()

    degrankG1 = degG1.values
    degrankG2 = degG2.values

    node_rank1 = rank_nodesG1.filter(lambda pair:pair[1]<seed_num+offset and pair[1]>=offset)\
                             .mapValues(lambda pair:pair-offset).collectAsMap()

    node_rank2 = rank_nodesG2.filter(lambda pair:pair[1]<seed_num+offset and pair[1]>=offset)\
                             .mapValues(lambda pair:pair-offset).collectAsMap()

    rank_node1 = {val:key for (key, val) in node_rank1.items()}
    rank_node2 = {val:key for (key, val) in node_rank2.items()}

    vector_labelsG1 = G1.join(degG1).coalesce(PARTS).values()\
                        .filter(lambda pair:pair[0] in node_rank1.keys())\
                        .mapValues(lambda x:[x]).reduceByKey(add)\
                        .join(degG1).coalesce(PARTS)\
                        .mapValues(lambda vec_d:[vec_d[1]]+sorted(vec_d[0],reverse=True))\
                        .collect()


    vector_labelsG2 = G2.join(degG2).coalesce(PARTS).values()\
                        .filter(lambda pair:pair[0] in node_rank2.keys())\
                        .mapValues(lambda x:[x]).reduceByKey(add)\
                        .join(degG2).coalesce(PARTS)\
                        .mapValues(lambda vec_d:[vec_d[1]]+sorted(vec_d[0],reverse=True))\
                        .collect()

    possible_connections={}
    
    for i in vector_labelsG1:
        for j in vector_labelsG2:
            possible_connections[(node_rank1[i[0]],node_rank2[j[0]])] = metric(i[1],j[1])


    cost_matrix = zeros([len(node_rank1),len(node_rank2)])
    for i in sorted(node_rank1.values()):
        for j in sorted(node_rank2.values()):
            cost_matrix[i,j]=possible_connections[(i,j)]
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    seeds_nodes1 = list(map(lambda x:rank_node1[x],row_ind))
    seeds_nodes2 = list(map(lambda x:rank_node2[x],col_ind))

    S = zip(seeds_nodes1,seeds_nodes2)
    seeds = sc.parallelize(S)

    return seeds


# DiNoiSe
def distributed_noisy_seeds(sc,G1,G2,seeds,PARTS,thres=2):

    M1 = sc.emptyRDD(); M2 = sc.emptyRDD()

    first_iter=True

    matching = sc.emptyRDD()

    matchcount=0;
    true_pos=0;
    i=0; 

    while not seeds.isEmpty():

        if first_iter:
            ###################
            # FIRST ITERATION #
            ###################
            a=time()
            S=seeds.collectAsMap()

            subgraphG1=G1.filter(lambda pair:pair[0] in S.keys())

            subgraphG2=G2.filter(lambda pair:pair[0] in S.values())

            subgraphG1=subgraphG1.map(lambda pair:(S[pair[0]],pair[1]))
    
            unmatched_neighbouring_pairs = \
                        subgraphG1.join(subgraphG2).values()\
                        .filter(lambda pair:pair[0] not in S.keys() and pair[1] not in S.values())\
                        .map(lambda x:(encode_tuple(x),1))\
                        .partitionBy(PARTS)

            first_iter=False


        else:
            a=time()
            ####################
            # 2...n ITERATIONS #
            ####################
            unmatched_neighbouring_pairs = seeds.join(G1).values().coalesce(PARTS)\
                                                .join(G2).values().coalesce(PARTS)\
                                                .map(lambda x:(encode_tuple(x),1))\
                                                .partitionBy(PARTS)
                                                


        ################################
        # CALCULATE SCORE/CHOOSE PAIRS #
        ################################
        pairs_with_score = unmatched_neighbouring_pairs.reduceByKey(add)

        candidates = pairs_with_score\
                    .filter(lambda edge_score:edge_score[1]>=thres)\
                    .map(lambda edge_score:(decode_tuple(edge_score[0]),edge_score[1]))\
                    .coalesce(PARTS)

        seeds = node_resolution(candidates,PARTS).keys().coalesce(PARTS)

        matching = matching.union(seeds).repartition(PARTS)
        
        
        ########################
        # REMOVE MATCHED NODES #
        ########################
        matched_nodesG1 = seeds.keys().map(lambda k:(k,None))
        matched_nodesG2 = seeds.values().map(lambda k:(k,None))

        G1 = G1.map(swap).subtractByKey(matched_nodesG1).map(swap).coalesce(PARTS)
        G2 = G2.map(swap).subtractByKey(matched_nodesG2).map(swap).coalesce(PARTS)


        ############
        # PROGRESS #
        ############
        matchcount += seeds.count()
        true_pos   += seeds.filter(lambda pair:pair[0]==pair[1]).count()

        i+=1
        b=time()
        print("matched_pairs:"+str(true_pos)+"|"+str(matchcount))
        print("iter:"+str(i))
        print("time (sec):"+str(b-a)+"\n\n")


    return matching


#+++ MAIN CLASS +++
def main(args):

    sc = SparkContext(appName="PGM")

    graph1 = sc.textFile(args.IN[0]).map(line_to_edge)
    graph2 = sc.textFile(args.IN[1]).map(line_to_edge)

    graph_name = args.IN[0]

    seed_num   = args.sn
    PARTS      = args.PARTS

    G1=deep_copy(graph1,PARTS)
    G2=deep_copy(graph2,PARTS)

    if args.inseeds:
        seeds = sc.textFile(args.inseeds).map(line_to_edge)
        matchtype="seeded"
        ETA=0

    else:
        matchtype="seedless"
        start=time()
        seeds = seed_generator(sc,G1,G2,seed_num,PARTS)
        stop=time()
        ETA=round(float(stop-start)/60,4)
        seeds2 = seeds.map(lambda pair:str(pair[0])+" "+str(pair[1]))
        seeds2.coalesce(1).saveAsTextFile(args.OUT+"segen_seeds")

    start=time()
    res = distributed_noisy_seeds(sc,G1,G2,seeds,PARTS)
    stop=time()
    ETB=round(float(stop-start)/60,4)
    res2 = res.map(lambda pair:str(pair[0])+" "+str(pair[1]))
    res2.coalesce(1).saveAsTextFile(args.OUT+matchtype+"_matching")
    print("\nSeGen   time :"+str(ETA)+" min ")
    print("DiNoiSe time :"+str(ETB)+" min\n")
    return [ETA,ETB]
    sc.stop()


if __name__ == "__main__":

    parser=argparse.ArgumentParser(\
            description='graph matching via percolation graph matching')

    parser.add_argument('--input', nargs=2, type=str, dest="IN", \
                        help="[G1_dir,G2_dir]", required=True)

    parser.add_argument('--output', type=str, dest="OUT", \
                        help="output dir",default="OUT/")

    parser.add_argument('--parts', type=int, dest="PARTS", required=True, \
                        help="partitions in Spark")

    parser.add_argument('--seeds_num', type=int, dest="sn", \
                        help="connections subset based on top ranking seeds",default=0)

    parser.add_argument('--input_seeds', type=str, dest="inseeds", \
                        help="seeds_dir" )

    args = parser.parse_args()
    main(args)

