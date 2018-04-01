import sys


# input comes from STDIN (standard input)
for line in sys.stdin:
    pairs = line.split()
    print '%s\t%s' % (pairs, 1)
