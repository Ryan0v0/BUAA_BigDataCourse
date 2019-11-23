#!/usr/bin/python

import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    name = record[0]
    print '%s\t%s' % (name,1)
