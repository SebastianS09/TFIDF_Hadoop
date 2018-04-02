#!/usr/bin/python

import sys
import string
import re

subsamp = 0

# input comes from STDIN (standard input)
#We want to create a list of words

for line in sys.stdin:
    # remove leading and trailing whitespace, punctuation and caps
    line = line.strip()
    line = re.sub('['+string.punctuation+']', '', line)
    line = line.lower()

    # split the line into words
    words = line.split()

    # using the position in the book as key to avoid sorting alphabeltically in the shuffle and sort operation
    # subsampling to take advantage of shuffle and sort algorithm 
    for word in words:
        subsamp += 1
        print '%s\t%s\t%s' % (subsamp, word, 1)
        if subsamp > 10000:
            subsamp = 0
