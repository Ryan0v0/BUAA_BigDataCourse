'''
from __future__ import print_function
from pyspark import SparkContext
import json

sc = SparkContext( 'local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//2.json")

global mp
mp = dict()


def join1(r):
    if r[1] in mp.keys():
        return r
    return []


def add1(r):
    mp[r[1]] = r


tmp = textFile.filter(lambda row: json.loads(row)[0] == "order")
tmp.foreach(add1)
ans = textFile.filter(lambda row: json.loads(row)[0] == "line_item").flatMap(lambda row: join1(row))
ans.foreach(print)
'''

from __future__ import print_function
from pyspark import SparkContext
import json


def join1(items):
    res = []
    lft = []
    for item in items:
        if item[0] == "order":
            lft = item
            break
    if len(lft) == 0:
        return res
    for item in items:
        if item[0] == "line_item":
            res.append(lft + item)
    return res


sc = SparkContext('local', 'test')
textFile = sc.textFile("file:///root/bigdata/inputs//2.json")
ans = textFile.map(lambda row: json.loads(row)).map(lambda row: (row[1], row)).groupByKey()
ans = ans.flatMap(lambda items: join1(items[1]))
ans.foreach(print)
