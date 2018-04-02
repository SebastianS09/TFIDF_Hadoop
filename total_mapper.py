#!/usr/bin/python

import sys

#counting the total number of words per document (here it is a fixed size but in case we want to have add variability)
# input comes from STDIN (standard input), from the word_count mapper in the form 
# Word, Document, Count
# We keep only the document and the count to keep a track of the number of words per document

for line in sys.stdin:
    triplets = line.split()
    print '%s\t%s' % (triplets[1],triplets[2])
