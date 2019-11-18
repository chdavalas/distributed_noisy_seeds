# Distributed NoisySeeds
A map-reduce implementation of the NoisySeeds algorithm as described in the paper of kazemi et al. "Growing a graph from a handful 
of seeds". The Distributed NoisySeeds (DiNoiSe) algorithm works upon a cluster of commodity hardware algorithm whilst following 
the same logic as the original NoisySeeds algorithm. This implementation is written in PySpark (Python API for Apache Spark).

An addition to this implementation is an experimental seed generation algorithm for the purpose of solving the problem of finding 
side information named SeedGenerator (SeGen) algorithm. SeGen is an adaptation to the Weisfeiler-Lehman graph isomorphism 
1-dimensional test for graphs (also known as Naive Vertex Refinement).





# Background
The NoisySeeds is a Percolation Graph Matching (PGM) algorithm. PGM algorithm have been made with the purpose of acquiring an 
approximate solution to the graph matching problem. A common use of these algorithms is Network de-anonymization. More 
specifically, the algorithm can find shared users between two networks, which have been anonymized.

![](ns_step.png)





# Motivation
* Graph data which represent social netowrks are massive therefore the computations required from percolation graph matching can become prohibitive even for contemporary hardware.

* A PGM algorithm will require an initial set of pre-acquired connections (named seeds). The problem is that finding a priori 
knowledge can be demanding and often requires the human factor. An automatic system for finding initial knowledge can eliminate all 
possible difficulties.





# Citation
--Forthcoming--
