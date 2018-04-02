#!/usr/bin/python

import sys

# We want to aggregate all the information we have obtained previously
# here the word, the document id and the count per document

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the pairs into word, hash & key
    triplets = line.split()
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for IDF_reducer.py
        # tab-delimited; the trivial word count is 1
    print '%s\t%s\t%s\t%s' % (triplets[0],triplets[1],triplets[2],1)
