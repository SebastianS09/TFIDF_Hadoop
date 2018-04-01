import sys

file = open("totals")
test = file.readline()
test = test.split()

# input comes from STDIN (standard input)
for line in sys.stdin:
    print test
