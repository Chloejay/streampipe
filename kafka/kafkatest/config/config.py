import yaml
import json
import requests 

class Configuration:
    def __init__(self,filename):
        with open(filename, 'r') as f:
            self.doc = yaml.load(f, Loader=yaml.FullLoader)
        
    def getMySQLmetadata(self):
        server = self.doc["server"]
        host = server[0]['host']
        user = server[1]['user']
        passwd = server[2]['passwd']
        db = server[3]['db']

        return host,user,passwd,db

    def getBrokermetadata(self):
        broker = self.doc["broker"]
        host = broker[0]['host']
        port = broker[1]['port']

        return host,port
    
    def getConsumermetadata(self):
        consumer = self.doc["consumer"]
        topic = consumer[0]['topic']
        consumergroup = consumer[1]['consumergroup']

        return topic,consumergroup

    def getAvroSchema(self):
        avro = self.doc["avro"]
        schema = avro[0]['schema']
        
        return schema