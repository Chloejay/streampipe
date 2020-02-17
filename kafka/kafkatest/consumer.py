# !/streampipe/kafka/kafkatest

from kafka import KafkaConsumer
from config.config import Configuration
import pymysql
import io
import avro.schema
import avro.io
import traceback
import sys
import os
from producer import * 

conf = Configuration("kafkatest/config/default.yml")
host,user,passwd,db = conf.getMySQLmetadata()
kafka_host,kafka_port = conf.getBrokermetadata()
topic,consumergroup = conf.getConsumermetadata()
schema_avro_path = os.path.join('kafkatest/config/',conf.getAvroSchema()) 
broker_config=kafka_host+":"+str(kafka_port)
schema = avro.schema.Parse(open(schema_avro_path, 'r').read())

def consume_records(topic):
    consumer = KafkaConsumer(bootstrap_servers=[broker_config],
                                auto_offset_reset='earliest', #read all, default is latest 
                                enable_auto_commit= True, 
                                group_id='test-consumer-group', #to avoid message be consumed more than once 
                                consumer_timeout_ms= 120 
                                )

    consumer.subscribe([topic]) 

    for msg in consumer:
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        val = reader.read(decoder)
        test_val=list(val.values()) 
        print(test_val)

        #connect to RDBMS 
        conn = pymysql.connect(host,user,passwd,db)
        cursor =conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS kafkav1 (id int auto_increment primary key, \
        testing text, favorite_color text, favorite_number int)""")
        cursor.execute("""INSERT INTO kafkav1(testing,favorite_number,favorite_color) values \
        ('%s', %s, '%s')"""%(test_val[0],test_val[1],test_val[2])) 
        conn.commit()
        
        logging.info('send test data to mysql sink')
        conn.close() 

if __name__=='__main__':
    topic= "kafkatesting"
    consume_records(topic)