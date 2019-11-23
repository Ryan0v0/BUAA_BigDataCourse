#!/usr/bin/python
import sys
import json

for line in sys.stdin:
    line = line.strip()
    record = json.loads(line)
    words = []
    Type = record[0]
    Id = int(record[1])
    for word in record:
        words.append(word)
    print '%s\t%s' % (Id, json.dumps(words))
