#!/usr/bin/python
import sys
import random

#setting the initial reservoir size K to 200
count = 0
N = 0
K = 200

#generating a random key for the each document (probability of two documents having the same ID very small)
key = random.randint(0,10000)

#initialising the reservoir
save = []

# input comes from STDIN
for line in sys.stdin:
    #getting each word
    item = line.split('\t', 2)[1]
    
    #incrementing counter
    N += 1 
    
    #filling the reservoir with the first K items
    if count < K:
        save.append(item)
        count += 1
    
    #for the next items, replace with probability 1/N
    else: 
        s = random.randint(0,N)
        if s < K:
            save[s] = item
            
#output the word and the document it belongs to (key)
for i in save:
    print '%s\t%s' % (i,key)
