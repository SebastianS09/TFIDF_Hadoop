#!/usr/bin/python
import sys
import random

count = 0
N = 0
K = 200

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
