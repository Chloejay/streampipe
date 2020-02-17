#!streampipe/kafka 

from config.config import * 
from kafka import KafkaProducer, SimpleProducer, KafkaClient 
import io, os
import avro 
import avro.schema, avro.io
from avro.io import DatumWriter
import random 
import requests 
import logging
logging.basicConfig(level = logging.INFO)



#TODO, implement with REST API with confluentic kafka rest https://github.com/confluentinc/kafka-rest 

def create_producer(topic): 
    # producer = KafkaProducer(bootstrap_servers=['localhost:9092']) 
    conf = Configuration("kafkatest/config/default.yml") 
    #send messages synchronously
    KAFKA = KafkaClient('localhost:9092')
    producer = SimpleProducer(KAFKA)
    schema_path = os.path.join('kafkatest/config/',conf.getAvroSchema())
    schema = avro.schema.Parse(open(schema_path, 'r').read())

    for _ in range(10):
        writer = DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write({"testing": "test",
                    "favorite_color": random.choice(['red', 'green','yellow']),
                    "favorite_number": random.randint(1, 10)}, 
        encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.send_messages(topic, raw_bytes)
        logging.info('Publish message!')


if __name__=='__main__':
    topic='kafkatesting'
    create_producer(topic)