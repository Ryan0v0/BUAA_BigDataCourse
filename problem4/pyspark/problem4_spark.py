from __future__ import print_function
from pyspark import SparkContext
import json


def db(x):
    return [(x[0],x[1]),(x[1],x[0])]

sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//3&4.json")
ans = textFile.map(lambda row: json.loads(row)).flatMap(lambda row: db(row)).map(lambda item: (item, 1)).reduceByKey(lambda a,b:a+b).filter(lambda a:a[1]==1).keys().map(lambda x:list(x))
ans.foreach(print)
