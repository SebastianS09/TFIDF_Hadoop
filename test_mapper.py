import sys
import os 

test = os.environ["TOTALS"]

# input comes from STDIN (standard input)
for line in sys.stdin:
    pairs = line.split()
    print '%s\t%s' % (pairs, test)
