#!/usr/bin/python


import sys

current_doc = None
current_count = 0
word_count = 0
doc = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    doc, words = line.split('\t')
    
    #incrementing the number of words
    word_count += int(words)
    #incrementing the number of docs
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: doc) before it is passed to the reducer
    if current_doc != doc:
        current_count += 1
        current_doc = doc
        
# output the last line having the total count
if current_doc == doc:
    print '%s\t%s\t%s' % (current_count, word_count,"total_counts")
