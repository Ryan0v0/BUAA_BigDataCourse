# encoding=utf8
import sys
import csv
import io
import jieba
import re
from pyspark.ml.feature import Tokenizer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import functions
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF
from pyspark.sql.functions import udf, rand, col
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import StopWordsRemover
reload(sys)
sys.setdefaultencoding('utf8')

jieba.initialize()

def spark_read_csv_bf(spark, path, schema=None, encoding='utf8'):
    rdd = spark.sparkContext.binaryFiles(path).values()\
                .flatMap(lambda x: csv.DictReader(io.BytesIO(x)))\
                .map(lambda x : { k:v.decode(encoding) for  k,v in x.iteritems()})
    if schema:
        return spark.createDataFrame(rdd, schema)
    else:
        return rdd.toDF()

def content2words(data):
    text = data[0]
    rule = re.compile(u'[^\u4e00-\u9fa5]')
    line = re.sub(rule, '', text)
    line = " ".join(list(jieba.cut(line)))
    return (line, data[1])

spark = SparkSession.builder.appName('nlp').getOrCreate()

train_df = spark_read_csv_bf(spark, 'file:///home/root/emailclass/train.csv', ["content","label"])
train_df = train_df.filter(((train_df.label =='1') | (train_df.label == '0')))
train_df.printSchema()
train_df = train_df.withColumn('label', train_df.label.cast('int'))
train_df.show(5,False)

train_df = train_df.sample(False,0.01,0)

test_df = spark_read_csv_bf(spark, 'file:///home/root/emailclass/test.csv', ["content"])
test_df.printSchema()
test_df = test_df.withColumn('label',functions.lit(0))
test_df.show(5,False)

test_df = test_df.sample(False, 0.01, 0)


rdd = train_df.rdd
rdd = rdd.map(content2words)
train_df = spark.createDataFrame(rdd, ["content", "label"])
train_df.show(1, False)

rdd = test_df.rdd
rdd = rdd.map(content2words)
test_df = spark.createDataFrame(rdd, ["content", "label"])
test_df.show(1, False)

tokenization = Tokenizer(inputCol='content', outputCol='token')
train_df = tokenization.transform(train_df)
test_df = tokenization.transform(test_df)

stopfile = io.open("stopwords.txt", "r", encoding="utf8")
stopwordlist = stopfile.read().splitlines()
stopfile.close()

sw_removal = StopWordsRemover(inputCol='token', outputCol='new_token').setStopWords(stopwordlist)
train_df  = sw_removal.transform(train_df)
test_df = sw_removal.transform(test_df)

train_df.select(['token','new_token']).show(1, False)
test_df.select(['token','new_token']).show(1, False)


token_count = udf(lambda x: len(x), IntegerType())

train_df = train_df.withColumn('token_count', token_count(col('new_token')))
test_df = test_df.withColumn('token_count', token_count(col('new_token')))


tf_vector = HashingTF(inputCol='new_token', outputCol='tf_vector')

train_tf_vec = tf_vector.transform(train_df)
test_tf_vec = tf_vector.transform(test_df)


tfidf_vector = IDF(inputCol='tf_vector', outputCol='tfidf_vector')

train_tfidf_vec = tfidf_vector.fit(train_tf_vec).transform(train_tf_vec)
test_tfidf_vec = tfidf_vector.fit(test_tf_vec).transform(test_tf_vec)


assembler = VectorAssembler(inputCols=['tfidf_vector','token_count'], outputCol='X')

train_tfidf_vec = assembler.transform(train_tfidf_vec)
test_tfidf_vec = assembler.transform(test_tfidf_vec)


train_data, dev_data = train_tfidf_vec.randomSplit([0.95, 0.05])

model = LogisticRegression(featuresCol='X', labelCol='label').fit(train_data)

result_dev = model.evaluate(dev_data).predictions
result_test = model.evaluate(test_tfidf_vec).predictions

result_test = result_test.withColumn('final', result_test.prediction.cast('int'))
result_test.select("final").write.csv(path="file:///home/root/emailclass/sub_1.csv", header="false")

auc_dev = BinaryClassificationEvaluator(labelCol='label').evaluate(result_dev)

print(auc_dev)

