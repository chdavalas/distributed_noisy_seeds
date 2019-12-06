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



import dinoise
import dinoise_w_bucketing
import argparse

from pyspark.context import SparkContext
from pyspark.storagelevel import StorageLevel
from time import time
from operator import add


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





def evaluate_output(fname,G1,G2,res,save_filename,et,PARTS):

    failsafe1 = res.mapValues(lambda x:[x]).reduceByKey(add)\
            .filter(lambda x:len(x[1])==2)


    failsafe2 = res.map(swap).mapValues(lambda x:[x]).reduceByKey(add)\
            .filter(lambda x:len(x[1])==2)


    if not failsafe1.isEmpty() or not failsafe2.isEmpty(): 
        print(failsafe1.collect())
        print(failsafe2.collect())
        print("///-----------------------///")
        print("///-----------------------///")
        print("///-----------------------///")
        print("///----COLLISION_ERROR----///")
        print("///-----------------------///")
        print("///-----------------------///")
        print("///-----------------------///")
        quit()

    degG1 = G1.mapValues(lambda x:1).reduceByKey(add)
    degG2 = G2.mapValues(lambda x:1).reduceByKey(add)

    nodes=min(degG1.keys().count(),degG2.keys().count())

    N1 = degG1.filter(lambda pair: pair[1]>=2)
    N2 = degG2.filter(lambda pair: pair[1]>=2)
    N_ident = N1.keys().intersection(N2.keys()).count()

    N_matched      = res.count()
    true_positives = res.filter(lambda pair: pair[0]==pair[1]).count()

    perc_rate = round(float(N_matched)/nodes,3)

    if N_matched==0: pre = 0
    else           : pre = round(float(true_positives)/N_matched,3)

    if N_ident==0: rec = 0
    else         : rec = round(float(true_positives)/N_ident,3)

    if pre==0 and rec==0 :F1 = 0
    else                 :F1 = round(2*float(pre*rec)/(pre+rec),3)

    print("\n-Percolation rate : "+str(perc_rate*100)+"% ("+str(N_matched)\
          +'/'+str(nodes)+")")

    print("-Precision        : "+str(pre*100)+"% ("+str(true_positives)\
          +'/'+str(N_matched)+")")

    print("-Recall           : "+str(rec*100)+"% ("+str(true_positives)\
          +'/'+str(N_ident)+")")

    print("-F1-Score         : "+str(F1)+"/1\n\n")

    #print save_filename

    stats="\n"+str(fname)+","+str(et)+","+str(perc_rate)+","+str(pre)+","+str(rec)+","+str(F1)+"\n"

    with open(save_filename,"a+") as f:
        f.write(stats)
    print("\n\n\n [Time elapsed: "+str(et)+" min] \n\n\n")
    return stats




def main(args):

    sc = SparkContext(appName="PGM")

    graph1 = sc.textFile(args.IN[0]).map(line_to_edge)
    graph2 = sc.textFile(args.IN[1]).map(line_to_edge)

    graph_name = args.IN[0]

    seed_num   = args.sn
    PARTS      = args.PARTS

    G1=deep_copy(graph1,PARTS)
    G2=deep_copy(graph2,PARTS)

    IsBucket  = ""
    matchtype = ""

    if args.inseeds:
        seeds = sc.textFile(args.inseeds).map(line_to_edge)
        matchtype="_seeded_"
        ETA=0

    else:
        matchtype="_seedless_"
        start=time()
        seeds = dinoise.seed_generator(sc,G1,G2,seed_num,PARTS)
        stop=time()
        ETA=round(float(stop-start)/60,4)
        stats=evaluate_output(graph_name+matchtype+IsBucket,G1,G2,seeds,"seeds_log.csv",ETA,PARTS)

    if not args.bucketing:

        start=time()
        res = dinoise.distributed_noisy_seeds(sc,G1,G2,seeds,PARTS)
        stop=time()
    
    else:
    
        start=time()
        res = dinoise_w_bucketing.distributed_noisy_seeds(sc,G1,G2,seeds,PARTS)
        IsBucket = "_bucket_"
        stop=time()
    
    ETB=round(float(stop-start)/60,4)
    stats=evaluate_output(graph_name+matchtype+IsBucket,G1,G2,res,"results_log.csv",ETB,PARTS)

    sc.stop()






if __name__ == "__main__":

    parser=argparse.ArgumentParser(\
            description='graph matching via percolation graph matching')

    parser.add_argument('--input', nargs=2, type=str, dest="IN", \
                        help="[G1_dir,G2_dir]", required=True)

    parser.add_argument('--parts', type=int, dest="PARTS", required=True, \
                        help="partitions in Spark")

    parser.add_argument('--seeds_num', type=int, dest="sn", \
                        help="connections subset based on top ranking seeds",default=0)

    parser.add_argument('--input_seeds', type=str, dest="inseeds", \
                        help="seeds_dir" )

    parser.add_argument('--bucketing', help="bucketing feature", \
                        action="store_true" )

    args = parser.parse_args()
    main(args)
