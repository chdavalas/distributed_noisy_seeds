### Distributed NoisySeeds ###
A map-reduce implementation of the NoisySeeds algorithm as described in the paper of kazemi et al. "Growing a graph from a handful 
of seeds". The Distributed NoisySeeds (DiNoiSe) algorithm works upon a cluster of commodity hardware algorithm whilst following 
the same logic as the original NoisySeeds algorithm. This implementation is written in PySpark (Python API for Apache Spark).

An addition to this implementation is an experimental seed generation algorithm for the purpose of solving the problem of finding 
side information named SeedGenerator (SeGen) algorithm. SeGen is an adaptation to the Weisfeiler-Lehman graph isomorphism 
1-dimensional test for graphs (also known as Naive Vertex Refinement).


<br/>


### Background ###
The NoisySeeds is a Percolation Graph Matching (PGM) algorithm. PGM algorithm have been made with the purpose of acquiring an 
approximate solution to the graph matching problem. A common use of these algorithms is Network de-anonymization. More 
specifically, the algorithm can find shared users between two networks, which have been anonymized.

![](ns_step.png)


<br/>


### Motivation ###
* Graph data which represent social netowrks are massive therefore the computations required from percolation graph matching can become prohibitive even for contemporary hardware.

* A PGM algorithm will require an initial set of pre-acquired connections (named seeds). The problem is that finding a priori 
knowledge can be demanding and often requires the human factor. An automatic system for finding initial knowledge can eliminate all 
possible difficulties.


<br/>


### Instructions ###
You can use the dinoise algorithm with the help of an isolated instance of python3

1. Set the python environment

  * __clone project__<br/> git clone https://github.com/chdavalas/distributed_noisy_seeds.git<br/>
  
  * __change directory to project folder__<br/> cd [parent/directory]/distributed_noisy_seeds<br/>
  
  * __ensure python-pip has been installed__<br/> sudo apt-get install python3-pip<br/>
  
  * __ensure virtualenv has been installed__<br/> pip3 install virtualenv<br/>
  
  * __create new python3 environment with virtualenv__<br/> which python3; virtualenv -p -python dir- env<br/>

  * __activate environment source__<br/> env/bin/activate<br/>
  
  * __install suggested requirements and check if properly installed__<br/> pip3 install -r requirements.txt; pip3 freeze<br/>


2. Run tests

  * __make testing script executable__<br/> chmod +x run_test_data<br/>

<br/>


# Citation
--Forthcoming--
