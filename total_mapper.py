#!/usr/bin/python

import sys

#counting the total number of documents 
#counting the total number of words

# input comes from STDIN (standard input)
for line in sys.stdin:
    triplets = line.split()

    print '%s\t%s' % (triplets[1:2])
