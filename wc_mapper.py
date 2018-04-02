#!/usr/bin/python

import sys

#using input already in (word,key) format: here word and document number
#but we need:
# 1) to add 1 as key for the word count reducer
# 2) to have same words from within the same document together: thus concatening word_docnb which will then be sorted
# example (cat, 4000), (cat,4003), (cat,3000), (cat,4003) will give:
# (cat_3000,1), (cat_4000,1), (cat_4003,1), (cat_4003,1) thus allowing to properly count by word and document

# input comes from STDIN (standard input)
for line in sys.stdin:
    # split the pairs into word and key
    pairs = line.split()
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        # Keeping the ID as mentionned above
        # tab-delimited; the trivial word count is 1
    print '%s\t%s' % (pairs[0]+'_'+pairs[1], 1)

   
