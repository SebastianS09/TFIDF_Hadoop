#!/usr/bin/python


import sys

current_word = None
current_count = 0
current_occ = 0
occ_count = 0
word = None

file = open('part-00000','r')
doc_numb = file.read()[0]


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from IDF_mapper.py
    word, ID, count, occ = line.split('\t')

    # convert count (currently a string) to int
    try:
        occ = int(occ)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_occ += 1
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, float(current_occ)/doc_numb)
        current_word = word
        current_occ = 1

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, float(current_occ)/doc_numb)
