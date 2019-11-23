#!/usr/bin/python

import sys
result= {}
for line in sys.stdin:
    line = line.strip()
    trim,Id = line.split('\t', 1)
    try:
        result[trim] = 0
    except ValueError:
        pass
for word in result:
    print '%s' % (word)
