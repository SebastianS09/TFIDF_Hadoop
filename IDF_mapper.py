#!/usr/bin/python

import sys

#counting the number of documents where a word occurs in a given corpus
#take the wordcount with hash (distinct words in documents appear multiple tumes)
#counting the number of occurences amounts to recounting the words from the word count output without the hash

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the pairs into word, hash & key
    triplets = line.split()
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        # tab-delimited; the trivial word count is 1
    print '%s\t%s\t%s\t%s' % (triplets[0],triplets[1],triplets[2],1)
