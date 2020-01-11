#!streampipe/kafka/kafkatest 

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, DataFrame, SQLContext
from pyspark.sql import SparkSession 


def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        sqlContext = getSqlContextInstance(rdd.context)
        rowRdd = rdd.map(lambda w: Row(word=w[0], cnt=w[1]))
        wordsDataFrame = sqlContext.createDataFrame(rowRdd)
        wordsDataFrame.show()

        wordsDataFrame.createOrReplaceTempView("words")
        wordCountsDataFrame = spark.sql("select SUM(cnt) as total from words")
        wordCountsDataFrame.show()

    except Exception as e:
        print('error is'.format(e)) 

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_df.py <zk> <topic>", file=sys.stderr)
        exit(-1)


export JAVA_HOME=/Users/chloeji/jdk-11.0.5.jdk/Contents/Home/



    sc = SparkContext.getOrCreate() 
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 100)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    words = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    words.foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()
