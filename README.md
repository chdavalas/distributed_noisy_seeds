# Distributed NoisySeeds
A map-reduce implementation of the NoisySeeds algorithm as described in the paper of kazemi et al. "Growing a graph from a handful 
of seeds". The Distributed NoisySeeds (DiNoiSe) algorithm works upon a cluster of commodity hardware algorithm whilst following 
the same logic as the original NoisySeeds algorithm. This implementation is written in PySpark (Python API for Apache Spark).

An addition to this implementation is an experimental seed generation algorithm for the purpose of solving the problem of finding 
side information named SeedGenerator (SeGen) algorithm. SeGen is an adaptation to the Weisfeiler-Lehman graph isomorphism 
1-dimensional test for graphs (also known as Naive Vertex Refinement).


<br/>


# Background
The NoisySeeds is a Percolation Graph Matching (PGM) algorithm. PGM algorithm have been made with the purpose of acquiring an 
approximate solution to the graph matching problem. A common use of these algorithms is Network de-anonymization. More 
specifically, the algorithm can find shared users between two networks, which have been anonymized.

![](ns_step.png)


<br/>


# Motivation
* Graph data which represent social netowrks are massive therefore the computations required from percolation graph matching can become prohibitive even for contemporary hardware.

* A PGM algorithm will require an initial set of pre-acquired connections (named seeds). The problem is that finding a priori 
knowledge can be demanding and often requires the human factor. An automatic system for finding initial knowledge can eliminate all 
possible difficulties.


<br/>


# Instructions
*Set environment<br/>

You can use the dinoise algorithm with the help of an isolated instance of python3

  * clone project<br/>
  git clone https://github.com/chdavalas/distributed_noisy_seeds.git<br/>
  
  * change directory to project folder<br/>
  cd [parent/directory]/distributed_noisy_seeds<br/>
  
  * ensure python-pip has been installed<br/>
  sudo apt-get install python3-pip<br/>
  
  * ensure virtualenv has been installed<br/>
  pip3 install virtualenv<br/>
  
  * create new python3 environment with virtualenv<br/>
  which python3<br/>
  virtualenv -p -python dir- env<br/>

  * activate environment<br/>
  source env/bin/activate<br/>
  
  * install suggested requirements and check if properly installed<br/>
  pip3 install -r requirements.txt<br/>
  pip3 freeze<br/>


* run tests<br/>

* make testing script executable<br/>
chmod +x run_test_data<br/>

<br/>


# Citation
--Forthcoming--
