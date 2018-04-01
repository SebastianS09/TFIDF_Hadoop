#!/usr/bin/python

import sys

#using input already in (word,key) format, but needing to have 1 as key for word count. Thus having the word and the ID concatenated

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the pairs into word and key
    pairs = line.split()
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        # Keeping the ID for future reference
        # tab-delimited; the trivial word count is 1
    print '%s\t%s' % (pairs[0]+'_'+pairs[1], 1)

   
