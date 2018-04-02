#!/usr/bin/python

########
# We need to access the data of the number of words per document for computing term frequency and the total number of
# documents for computing IDF
# Given that this file is very small (1 line per document) we can use hadoop integrated distributed cache system.
# This cache is accessible by all the nodes, and the reducer can thus access the needed information
# We use the commande -cacheFile to cache the output from Total_reducer.py (only one file because we specified one reducer only) 
# The file can then has the name after the # (here the same name part-00000) and can be read in a traditionnal manner (see below)
#######
# We also need to now the occurence of a word in the corpus and its frequency per document
# To obtain this information in a single step, we store the data for each word temporarily in a temp list
# The algorithm goes through the lines and appends them in temp
# Ex: [(cat, 3000, 5, 1) (cat, 3004, 3, 2, 1) (cat 4006, 10, 5, 1)] -> after looping through 'cat',
#      we know that cat appears in 3 documents and as such that the IDF is log(10/3), which we can then immediatly multiply with
#      the frequency of the given word still stored in temp
#######

import sys
import math

current_word = None
current_count = 0
current_occ = 0
occ_count = 0
word = None

# reading the distributed cache
with open('part-00000','r') as file:
    stats = file.readlines()

# aggregating this information in a dictionnary 
## {'document_id': number of words}
stats = [i.split() for i in stats]
stat_dict = dict(stats)

## length of dictionnary is total number of documents
doc_numb = len(stat_dict)

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from IDF_mapper.py
    word, ID, count, occ = line.split('\t')

    # convert count and occ (currently a string) to int
    try:
        occ = int(occ)
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    
    # count the number of documents a word appears in
    if current_word == word:
        current_occ += 1
        #storing the information for computing TF directly 
        temp.append((word,ID,count))
    
    # when changing words:
    else:
        if current_word:
            # for each element in the temporary list, compute the tfidf score based on 
            # a lookup in the dictionnary to get the number of words per document and thus the word frequency
            # the total number of documents
            # the number of documents in which the words appear 
            for line in temp:
                print '%s\t%s\t%s' % (line[0], line[1], line[2]/float(stat_dict[line[1]])*(math.log(float(doc_numb)/current_occ)))

        # resest variables
        current_word = word
        current_occ = 1
        
        temp = [(word,ID,count)]

#print last word if needed
if current_word == word:
    for line in temp:
        print '%s\t%s\t%s' % (line[0], line[1], line[2]/float(stat_dict[line[1]])*(math.log(float(doc_numb)/current_occ)))
