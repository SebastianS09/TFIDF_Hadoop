#!/usr/bin/python

import sys
import random
import os

######
#Special Version of the reducer taking advantage of the -cmdenv option of Hadoop streaming:
#the parameter K is given actually given in the hadoop mapreduce jar through -cmdenv K=500 for example
#It is then passed as a new OS environment variable which is accessible by the nodes (here $K) and thus to python using the os library
#It allows to run testing script much more flexibly by simply passing a parameter
#######

count = 0
N = 0
K = os.environ["K"]
K = int(K)

key = random.randint(0,10000)
save = []

# input comes from STDIN
for line in sys.stdin:
    item = line.split('\t', 2)[1]

    N += 1 
    if count < K:
        save.append(item)
        count += 1
    else: 
        s = random.randint(0,N)
        if s < K:
            save[s] = item

for i in save:
  print '%s\t%s' % (i,key)
