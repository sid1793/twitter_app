#!/bin/env python
# encoding: utf-8

import json
import time
import math
import threading
import signal
import sys
from flask import Flask
from flask import request
from flask import abort
from flask import url_for
from flask import make_response
from flask import Response
from pymongo import MongoClient
from bson import json_util
from threading import Thread
from pymongo import CursorType


def signal_handler(signal, frame):
    print 'You pressed Ctrl+C!'
    sys.exit(0)


def event_stream():
    print "Begins to retrieve tweets from the Mongo database..."
    db = MongoClient().tstream
    coll = db.tweets
    cursor = coll.find({"coordinates.type" : "Point" }, cursor_type = CursorType.TAILABLE_AWAIT)
    ci=0
    while cursor.alive:
        try:
            doc = cursor.next()
            ci += 1
            message = json.dumps(doc, default=json_util.default)
            yield 'data: %s\n\n' % message
        except StopIteration:
            pass


def subscribe_sns(topic_arn):
    #Subscribe


    #Confirm subscription



    
app = Flask(__name__)
@app.route('/tweets')
def tweets():  
    url_for('static', filename='map.html')
    url_for('static', filename='jquery-1.7.2.min.js')
    url_for('static', filename='jquery.eventsource.js')
    url_for('static', filename='jquery-1.7.2.js')
    return Response(event_stream(), headers={'Content-Type':'text/event-stream'})
 

if __name__ == '__main__':
    #signal.signal(signal.SIGINT, signal_handler)
    #app.before_first_request(runThread)
    app.run(debug=True, host='0.0.0.0')  
