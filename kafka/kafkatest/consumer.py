# !/streampipe/kafka/kafkatest
from kafka import KafkaConsumer
from config.config import Configuration
import pymysql
import logging
import io
import avro.schema
import avro.io
import traceback
import json
import time
import sys
import os


conf = Configuration("kafkatest/config/default.yml")
host,user,passwd,db = conf.getMySQLmetadata()
kafka_host,kafka_port = conf.getBrokermetadata()
topic,consumergroup = conf.getConsumermetadata()
schemaAvro = conf.getAvroSchema()
broker_config=kafka_host+":"+str(kafka_port)

def run():
    consumer = KafkaConsumer(bootstrap_servers=[broker_config],
                                auto_offset_reset='earliest',
                                enable_auto_commit= True,
                                group_id=consumergroup)
                                # value_deserializer=lambda x: loads(x.decode('utf-8'))

    consumer.subscribe(['kafkatest'])
    for message in consumer:
        print(str(message))
        # data=json.loads(message.value)

        conn = pymysql.connect(host,user,passwd,db)
        cursor =conn.cursor()
        cursor.execute("INSERT INTO whatever (stations) values (%s)"%(message.offset))
        # cursor.execute("INSERT INTO whatever (test) values (%s)"%(data))

        conn.commit()
        logging.info('Use kafka as the sink to load data to DWH, this case is mysql')
    conn.close()

# schema = avro.schema.parse(open(schemaAvro).read())
        # bytes_reader = io.BytesIO(message.value)
        # decoder = avro.io.BinaryDecoder(bytes_reader)
        # # reader = avro.io.DatumReader(schema)
        # # user1 = reader.read(decoder)
        # insertIntoDatabase(bytes_reader)

if __name__=='__main__':
    run()

# #TODO : implement with JDBC driver
# # curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
# #         "name": "jdbc_source_mysql_01",
# #         "config": {
# #                 "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
# #                 "connection.url": "jdbc:mysql://mysql:3306/kafka_test",
# #                 "connection.user": "user",
# #                 "connection.password": "password",
# #                 # "topic.prefix": "mysql-01-",
# #                 "mode":"bulk",
# #                 "poll.interval.ms" : 3600000
# #                 }
# #         }'

# # mysql-connector-java-8.0.13.jar
