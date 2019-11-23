#qzznb!
from __future__ import print_function
from pyspark import SparkContext
import json
def fun(record):
    if record[0] == 'a':
        return (record[2],record)
    elif record[0] == 'b':
        return (record[1],record)

def mul(lists):
    sz = len(lists)
    res = []
    for x in range(sz):
        for y in range(x+1,sz):
            #print(x,y)
            #print(lists[x][0],lists[y][0])
            
            if lists[x][0] == "a" and lists[y][0] == "b":
                res.append(((lists[x][1],lists[y][2]),lists[x][3]*lists[y][3]))
            elif lists[x][0] == "b" and lists[y][0] == "a":
                res.append(((lists[y][1],lists[x][2]),lists[x][3]*lists[y][3]))
            
    return res

sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//6.json")
ans = textFile.map(lambda row: fun(json.loads(row))).groupByKey().flatMap(lambda x: mul(list(x[1]))).reduceByKey(lambda x,y:x+y).map(lambda x: [[x[0][0],x[0][1]],x[1]])
ans.foreach(print)
