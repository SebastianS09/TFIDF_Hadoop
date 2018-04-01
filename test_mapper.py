import sys

with open("totals","r") as f:
    for line in f:
        print line.rstrip()
f.closed
