from __future__ import print_function
from pyspark import SparkContext
import json
sc = SparkContext( 'local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//3&4.json")
ans = textFile.map(lambda row:(json.loads(row)[0],1)).reduceByKey(lambda a,b:a+b)
ans.foreach(print)
