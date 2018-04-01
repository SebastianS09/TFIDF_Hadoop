#!/usr/bin/python

import sys
import math

current_word = None
current_count = 0
current_occ = 0
occ_count = 0
word = None

stats = []
file = open('part-00000','r')
stats = file.readline()

stat_dict = dict(stats)
doc_numb = stat_dict["total docs"]

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from IDF_mapper.py
    word, ID, count, occ = line.split('\t')

    # convert count (currently a string) to int
    try:
        occ = int(occ)
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer

    if current_word == word:
        current_occ += 1
        temp.append((word,ID,count))
    else:
        if current_word:
            for line in temp:
              print '%s\t%s\t%s' % (line[0], line[1], line[2]/float(stat_dict[line[1]])*(math.log(float(doc_numb)/current_occ)))
            # write result to STDOUT
            #print '%s\t%s' % (current_word, float(current_occ)/doc_numb)
        current_word = word
        current_occ = 1
        
        temp = [(word,ID,count)]

# do not forget to output the last word if needed!
if current_word == word:
    #print '%s\t%s' % (current_word, float(current_occ)/doc_numb)
    for line in temp:
      print '%s\t%s\t%s' % (line[0], line[1], line[2]/float(stat_dict[line[1]])*(math.log(float(doc_numb)/current_occ)))
