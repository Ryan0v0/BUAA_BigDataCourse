#!/usr/bin/python
import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    matrix = record[0]
    row = record[1]
    column = record[2]
    val = record[3]
    if matrix == "a":
        for i in xrange(0, 5):
            print '%s %s\t%s %s %s' % (row, i, matrix, column, val)
    elif matrix == "b":
        for i in xrange(0, 5):
            print '%s %s\t%s %s %s' % (i, column, matrix, row, val)
