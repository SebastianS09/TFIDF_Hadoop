#!/usr/bin/python


import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from wc_mapper.py with the id (thus having 2 spaces, we need to split from the end to keep the id) 
    word, count = line.split('\t')

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            out = current_word.split('_')
            print '%s\t%s\t%s' % (out[0],out[1], current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    out = current_word.split('_')
            print '%s\t%s\t%s' % (out[0],out[1], current_count)
