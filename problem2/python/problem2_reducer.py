#!/usr/bin/python
import sys
import json
order_list = {}
line_item = []
for line in sys.stdin:
    line = line.strip()
    Id, words = line.split('\t')
    record = json.loads(words)
    if record[0] == "order":
        order_list[Id] = record
    else:
        line_item.append(record)
for item in line_item:
    if order_list.get(item[1]) is None:
        continue
    else:
        print '%s' % json.dumps(order_list[item[1]] + item)
