#!/usr/bin/python

import sys
file = open('part-00000','r')
test = file.read()

for line in sys.stdin:
  print '%s\t%s' % (test,1)
#with open("totals","r") as f:
#    for line in f:
#        print line.rstrip()
#f.closed
