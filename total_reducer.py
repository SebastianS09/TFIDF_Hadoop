#!/usr/bin/python


import sys

current_doc = None
current_doc_count = 0
current_word_count = 0
word_count = 0
doc = None

# input comes from STDIN
current_doc = None
current_doc_count = 0
current_word_count = 0
word_count = 0
doc = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    doc, words = line.split('\t')
    
    try:
        words = int(words)
    except ValueError:
        continue
        
    if current_doc==doc:
        word_count += words
    #incrementing the number of words
    # this IF-switch only works because Hadoop sorts map output
    # by key (here: doc) before it is passed to the reducer
    else: 
        if current_doc:
          print '%s\t%s' % (current_doc, word_count)  
        word_count = words
        current_doc = doc
        #incrementing the number of docs
        
# output the last line having the total count
if current_doc == doc:
    print '%s\t%s' % (current_doc, word_count)
    #outputting the total number of doc
