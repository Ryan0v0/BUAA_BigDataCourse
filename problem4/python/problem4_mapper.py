#!/usr/bin/python

import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    person = record[0]
    friend = record[1]
    print '%s\t%s' % ((person,friend), 1)
    print '%s\t%s' % ((friend,person), 1)
