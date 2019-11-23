from __future__ import print_function
from pyspark import SparkContext
import json

sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//5.json")
ans = textFile.map(lambda row: json.loads(row)).map(lambda row: (row[1][:-10],1)).groupByKey().keys()
ans.foreach(print)
