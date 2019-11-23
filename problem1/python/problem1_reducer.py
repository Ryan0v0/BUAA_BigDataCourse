#!/usr/bin/python
import sys

word2count = {}
for line in sys.stdin:
    line = line.strip()
    word, fileName  = line.split('\t', 1)
    try:
        fileList=word2count.get(word, [])
        if fileName not in fileList:
            fileList.append(fileName)
        word2count[word] = fileList
    except ValueError:
        pass
for word in word2count:
	print '%s\t%s' % (word, word2count[word])
