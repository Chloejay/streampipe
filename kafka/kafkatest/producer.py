#!streampipe/kafka 

from config.config import * 
from kafka import KafkaProducer, SimpleProducer, KafkaClient 
import io, os
import avro 
from avro import schema 
import avro.io
from avro.io import DatumWriter
import random 
import requests 
import struct
import logging
logging.basicConfig(level = logging.INFO)

MAGIC_BYTE = 0
SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# https://github.com/confluentinc/confluent-kafka-python/blob/master/confluent_kafka/avro/serializer/message_serializer.py
class ContextStringIO(io.BytesIO):
    """
    Wrapper to allow use of StringIO via 'with' constructs.
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
        return False

def find_latest_schema(topic: str)-> str:
    subject= topic +'-value' 
    versions_res= requests.get(
        url= '{}/subjects/{}/versions'.format(SCHEMA_REGISTRY_URL, subject),
    headers={
            'Content-Type':'application/vnd.schemaregistry.v1+json',
        },
        )
    
    latest_version= versions_res.json()[-1]
    schema_res= requests.get(
        url= '{}/subjects/{}/versions/{}'.format(SCHEMA_REGISTRY_URL, subject, latest_version),
        headers={
                'Content-Type':'application/vnd.schemaregistry.v1+json',
            },
    )
    schema_res_json= schema_res.json() 
   
    return schema_res_json['id'], avro.schema.Parse(schema_res_json['schema'])

def encode_producer(topic: str, record: dict) -> str: 
    '''
    Given a parsed avro schema, encode a record for the given topic. The record is expected to be a dictionary.
    :param str topic: Topic name
    :param dict record: An object to serialize
    '''
    try: 
        schema_id, schema= find_latest_schema(topic)
        # print(schema_id, schema)
        if not schema:
            raise ('Schema does not exist')
    except Exception as e:
        logging.warning(e) 

    writer= avro.io.DatumWriter(schema)

    with ContextStringIO() as outf:
        #write the magic byte and schema ID in network byte order (big endian)
        outf.write(struct.pack('bI', MAGIC_BYTE, schema_id)) 
        #write the record to the rest of the buffer 
        encoder= avro.io.BinaryEncoder(outf)
        # print(encoder) 
        writer.write(record, encoder) 
        return outf.getvalue() 


def encode_writer(topic: str, _msg: str)-> str:
    KAFKA = KafkaClient('localhost:9092')
    producer = SimpleProducer(KAFKA)
    try: 
        producer.send_messages(topic, _msg)
        logging.info('Send message to topic {}'.format(topic)) 
    except Exception as e:
        logging.warning('Failed to send message to topic {}'.format(topic))

    #-------------------------------------------
    #the version use Avro without Kafka REST 

    # conf = Configuration("kafkatest/config/default.yml") 
    # #send messages synchronously
    # KAFKA = KafkaClient('localhost:9092')
    # producer = SimpleProducer(KAFKA)
    # schema_path = os.path.join('kafkatest/config/',conf.getAvroSchema())
    # schema = avro.schema.Parse(open(schema_path, 'r').read())

    # for _ in range(10):
    #     writer = DatumWriter(schema)
    #     bytes_writer = io.BytesIO()
    #     #create encoder 
    #     encoder = avro.io.BinaryEncoder(bytes_writer)
    #     writer.write({"testing": "test-",
    #                 "favorite_color": random.choice(['red', 'green','yellow']),
    #                 "favorite_number": random.randint(1, 10)}, 
    #                 encoder)
    #     # writer.close() 
    #     raw_bytes = bytes_writer.getvalue()
    #     try:
    #         producer.create_producer(topic, raw_bytes)
    #         logging.info('Publish message!')
    #     except Exception:
    #         logging.warning('Failed to produce message!')

if __name__=='__main__':
    topic='avrofix'
    # find_latest_schema(topic)
    msg= encode_producer(topic, dict(testing='avrobytes', favorite_number=1, favorite_color='green'))
    encode_writer(topic, msg) 