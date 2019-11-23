#!/usr/bin/python
import sys

word2count = {}
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        count = int(count)
        word2count[word] = word2count.get(word, 0) + count
    except ValueError:
        pass
for word in word2count:
    print '%s\t%s' % (word, word2count[word])
