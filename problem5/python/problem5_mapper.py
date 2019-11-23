#!/usr/bin/python
import sys
import json
for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    Id = record[0]
    nucleotide = record[1]
    trim = nucleotide[:-10]
    print '%s\t%s' % (trim, Id)
