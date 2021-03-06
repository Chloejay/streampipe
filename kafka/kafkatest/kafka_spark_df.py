#!streampipe/kafka/kafkatest 

import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, DataFrame, SQLContext, SparkSession 
from pyspark.sql.functions import col, desc, upper, coalesce, count 
from pyspark.sql.types import StructField, StructType, StringType,IntegerType #schema 

# https://github.com/apache/spark/blob/v2.2.1/examples/src/main/python/streaming/direct_kafka_wordcount.py#L48
def process(rdd):
    try:
        rowRdd = rdd.map(lambda w: Row(color=w[0],num=w[1]))
        df = sqlContext.createDataFrame([rowRdd]).cache() 
        # df.where(col('favorite_color'='yellow')).show(n=5) 
        df.orderBy(col('favorite_color').desc())\
        .select(upper(coalesce(col('favorite_color')))\
        .alias('fave'))\
        .show(5)  
        df.groupby(col('favorite_color'))\
        .agg(count(col('num')).alias('count')).show() 
        df.createOrReplaceTempView("count") 
        # df.printSchema() 
        df_count= sqlContext.sql("select SUM(num) as total from count")
        df_count.show() 

    except Exception as e:
        print('error is {}'.format(e)) 




if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_df.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext.getOrCreate() 
    sqlContext = SQLContext(sc)

    ssc = StreamingContext(sc, 10)
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})

    lines = kvs.map(lambda x: x[1])
    words = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a+b)
    words.pprint() 

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination() 


# TODO
# https://github.com/apache/spark/blob/master/docs/structured-streaming-kafka-integration.md
# create kafka source 
# spark = SparkSession \
#     .builder \
#     .appName("test") \
#     .getOrCreate()

# def read_kafka(topic):
#     df = (spark.read
#     .format("kafka") 
#     .option("kafka.bootstrap.servers", "localhost:9092") 
#     .option("subscribe",topic)
#     .option("startingOffsets", "earliest")
#     .option("endingOffsets", "latest") 
#     .load()) 
#     # df.writeStream.format("text").option("checkpointLocation", "test.txt").option("path","test2.txt").start()
#     # df.writeStream.outputMode("append").format("console").start()
#     # df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

#     return df

# read_kafka('kafkatesting') 