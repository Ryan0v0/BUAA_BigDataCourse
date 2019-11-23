from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext( 'local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//1.json")
'''
for line in testFile:
    record=json.loads(line)
    fileName=record[0]
    value=record[1]
    words=value.split()
    for word in words:
        #todo
wordCount = textFile.map(lambda line:line[1].split(' ')).map(lambda word:(word,line[0])).groupByKey()
'''
'''
ans = textFile.flatMap(lambda row:(json.loads(row)[1].split(' '),json.loads(row)[0]))

ans.foreach(print)
print("~~~~~~");

ans = textFile.flatMap(lambda row:(json.loads(row)[1].split(' '),json.loads(row)[0]).map(lambda word:(word[0],word[1])

ans.foreach(print)
print("~~~~~~");

ans = textFile.flatMap(lambda row:(json.loads(row)[1].split(' '),json.loads(row)[0]).map(lambda word:(word[0],word[1])).reduceByKey(lambda a,b:a+b)

ans.foreach(print)
print("~~~~~~");
'''
def split2(x,y):
    ret=[]
    for word in set(x.split(' ')):
        ret.append([word, y])
    return ret
ans = textFile.flatMap(lambda row: split2(json.loads(row)[1], json.loads(row)[0])).map(lambda word:(word[0],[word[1] ])).reduceByKey(lambda a,b:a+b)
ans.foreach(print)
#for item in ans:
#    print(item)
