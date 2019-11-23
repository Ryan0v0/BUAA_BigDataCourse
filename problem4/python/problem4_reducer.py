#!/usr/bin/python
import sys
word2count = {}
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    try:
        word2count[word] = word2count.get(word, 0) + 1
    except ValueError:
        pass
for word in word2count:
    if word2count[word] < 2:
	       print '%s' % (word)
