from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from kafka import SimpleProducer, KafkaClient 
import logging 
import configparser

consumer_key="" 
consumer_secret=""

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token=""
access_token_secret=""

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self, api):
        self.api= api 
        super().__init__() 
        kafka = KafkaClient("localhost:9092")
        producer = SimpleProducer(kafka)  

    def on_data(self, data):
        try:
            producer.send_messages('kafkatest',data.encode('utf-8'))
            print(data)
        except Exception as e:
            logging.info(e) 
        return True

    def on_error(self, status):
        logging.error(status) 



if __name__ == '__main__': 
    config = configparser.ConfigParser()
    config.read('twitter-app-credentials.txt')
    consumer_key = config['DEFAULT']['consumerKey']
    consumer_secret = config['DEFAULT']['consumerSecret']
    access_token = config['DEFAULT']['accessToken']
    access_token_secret = config['DEFAULT']['accessTokenSecret']

    # l = StdOutListener() 
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api= tweety.API(auth) 

    stream = Stream(auth, listener = TweeterStreamListener(api))
    stream.filter(track=['data science], languages=['en'])  
