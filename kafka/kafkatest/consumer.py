# !/streampipe/kafka/kafkatest


from kafka import KafkaConsumer
from config.config import Configuration
import pymysql
import logging
import io
import avro.schema
import avro.io
import traceback
import sys
import os


conf = Configuration("kafkatest/config/default.yml")
host,user,passwd,db = conf.getMySQLmetadata()
kafka_host,kafka_port = conf.getBrokermetadata()
topic,consumergroup = conf.getConsumermetadata()
schema_path = os.path.join('kafkatest/config/',conf.getAvroSchema()) 
broker_config=kafka_host+":"+str(kafka_port)
schema = avro.schema.Parse(open(schema_path, "r").read())



def run(topic):
    consumer = KafkaConsumer(bootstrap_servers=[broker_config],
                                auto_offset_reset='earliest',
                                enable_auto_commit= True)

    consumer.subscribe([topic]) 

    for msg in consumer:
        bytes_reader = io.BytesIO(msg.value)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        val = reader.read(decoder)
        test_val=list(val.values()) 
        print(test_val)

        conn = pymysql.connect(host,user,passwd,db)
        cursor =conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS kafkav1 (id int auto_increment primary key, \
        testing text, favorite_color text, favorite_number int)""")
        cursor.execute("""INSERT INTO kafkav1(testing,favorite_number,favorite_color) values \
        ('%s', %s, '%s')"""%(test_val[0],test_val[1],test_val[2])) 
        conn.commit()
        logging.info('send test data to mysql sink')
    conn.close() 

# schema = avro.schema.parse(open(schemaAvro).read())
# bytes_reader = io.BytesIO(message.value)
# decoder = avro.io.BinaryDecoder(bytes_reader)
# reader = avro.io.DatumReader(schema)
# user1 = reader.read(decoder)
# insertIntoDatabase(bytes_reader) 

if __name__=='__main__':
    test= "kafkatesting"
    run(test)  
