import argparse
import os
import json
import sys
import datetime
import pickle
import time
import boto
import boto.sqs
from boto.sqs.message import Message
from threading import Thread
import requests
import ast
from boto import sns


#AWS Auth
REGION = 'us-east-1'
conn = boto.sqs.connect_to_region(REGION)
tweets_queue = conn.get_queue('tweets_queue')
c_sns = boto.sns.connect_to_region(REGION)


#Monkeylearn Auth
MONKEYLEARN_TOKEN = os.environ.get('MONKEYLEARN_TOKEN')



def process_message(tweets_queue):
    """ Reads a message in the queue, makes a call to monkeylearn sentiment API and return a dict with the result"""

    m = tweets_queue.read() 
    if m is not None:
        message = ast.literal_eval(m.get_body())
        print message

        #Call to the sentiment API
        data = {
            'text_list': [message['text']]
        }
        response = requests.post(
            "https://api.monkeylearn.com/v2/classifiers/cl_qkjxv9Ly/classify/?",
            data=json.dumps(data),
            headers={'Authorization': "token " + MONKEYLEARN_TOKEN,
                    'Content-Type': 'application/json'})
         
        result = dict(json.loads(response.text))['result'][0][0]
        result['tweet_id'] = message['tweet_id']
        return result
        

def send_notification(message, topicarn):
    """ Send an SNS notification to a given topic """

    message = "hello Mr Eloi"
    message_subject = "trialBotoTRopic"
    publication = c_sns.publish(topicarn, message)
    print publication


# def worker_function(arg):
#     for i in range(arg):
#         print "running"
#         time.sleep(1)


# if __name__ == "__main__":
#     thread = Thread(target = worker_function, args = (10, ))
#     thread.start()
#     thread.join()
#     print "thread finished...exiting"