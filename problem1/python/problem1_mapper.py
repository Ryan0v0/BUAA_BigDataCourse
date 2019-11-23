#!/usr/bin/python

import sys
import json

for line in sys.stdin:
    line=line.strip()
    record=json.loads(line)
    fileName=record[0]
    value=record[1]
    words=value.split()
    for word in words:
        print '%s\t%s' % (word,fileName)
