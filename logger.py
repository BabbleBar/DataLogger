import datetime
import json
import os
import pprint
import threading
from datetime import timedelta
from functools import update_wrapper

import pika
import pymongo
from bson.json_util import dumps
from dateutil.parser import parse
from flask import Flask
from flask import make_response, request, current_app
from pymongo import MongoClient

app = Flask(__name__)


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)

    return decorator


@app.route("/ping")
def hello():
    return "pong"


@crossdomain(origin='*')
@app.route("/type/<data_type>/avg/minutes/<int:minutes>")
def avg(data_type, minutes):
    return dumps(db[data_type].aggregate(
        [
            {"$match": {'timestamp': {"$gt": datetime.datetime.now() - datetime.timedelta(minutes=minutes)}}},
            {"$group": {"_id": "$eui",
                        "avg": {"$avg": "$payload_int"},
                        "min": {"$min": "$payload_int"},
                        "max": {"$max": "$payload_int"},
                        "first_timestamp": {"$min": "$timestamp"},
                        "last_timestamp": {"$max": "$timestamp"},
                        "count": {"$sum": 1}
                        }}
        ]
    ))


@crossdomain(origin='*')
@app.route("/eui/<eui>/type/<data_type>/avg/minutes/<int:minutes>")
def avg_eui(eui, data_type, minutes):
    return dumps(db[data_type].aggregate(
        [
            {"$match": {'eui': eui,
                        'timestamp': {"$gt": datetime.datetime.now() - datetime.timedelta(minutes=minutes)}}},
            {"$group": {"_id": "$eui",
                        "avg": {"$avg": "$payload_int"},
                        "min": {"$min": "$payload_int"},
                        "max": {"$max": "$payload_int"},
                        "first_timestamp": {"$min": "$timestamp"},
                        "last_timestamp": {"$max": "$timestamp"},
                        "count": {"$sum": 1}
                        }}
        ]
    ))


@crossdomain(origin='*')
@app.route("/eui/<eui>/type/<data_type>/last/<int:limit>")
def full_data(eui, data_type, limit):
    cursor = db[data_type].find({'eui': eui}).sort([("timestamp", pymongo.DESCENDING)]).limit(limit)
    return dumps(cursor)


def get_pika_params():
    if 'VCAP_SERVICES' in os.environ:
        vcap_service = json.loads(os.environ['VCAP_SERVICES'])

        return pika.URLParameters(url=vcap_service['rabbitmq'][0]['credentials']['uri'])

    return pika.ConnectionParameters(host="localhost")


def get_mongo_uri():
    if 'VCAP_SERVICES' in os.environ:
        vcap_service = json.loads(os.environ['VCAP_SERVICES'])

        return vcap_service['mongodb'][0]['credentials']['uri']

    return "mongodb://localhost"


def get_mongo_db():
    if 'VCAP_SERVICES' in os.environ:
        vcap_service = json.loads(os.environ['VCAP_SERVICES'])

        return vcap_service['mongodb'][0]['credentials']['database']

    return "mongodb://localhost"


def receive_new_message(ch, method, properties, body):
    data = json.loads(body)
    print("##########")
    print("CHANNEL: %s" % (pprint.pformat(ch)))
    print("EUI: %s %s - %s: %s" % (data['DevEUI'],
                                   data['Time'],
                                   data['FPort'],
                                   data['payload_hex']
                                   ))
    result = db.full_data.insert_one(data)
    print("Inserted into MongoDB: ")
    pprint.pprint(result.inserted_id)


def receive_new_message_data(ch, method, properties, body):
    data = json.loads(body)
    print("##########")
    print("CHANNEL: %s" % (pprint.pformat(ch)))
    data['timestamp'] = parse(data['timestamp'])
    print("EUI: %s %s - %s: %s / %s " % (data['eui'],
                                         data['timestamp'],
                                         data['data_type'],
                                         data['payload_hex'],
                                         data['payload_int']
                                         ))
    result = db[data['data_type']].insert_one(data)
    print("Inserted into MongoDB: ")
    pprint.pprint(result.inserted_id)


def start_listener():
    channel = connection.channel()
    channel.exchange_declare(exchange='data_log', type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='data_log',
                       queue=queue_name)

    print('listener started')

    channel.basic_consume(receive_new_message,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


def start_listener_data():
    channel = connection.channel()
    channel.exchange_declare(exchange='data', type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='data',
                       queue=queue_name)

    print('listener started')

    channel.basic_consume(receive_new_message_data,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


if __name__ == "__main__":
    port = os.getenv('VCAP_APP_PORT', '5000')

    mongo_client = MongoClient(get_mongo_uri())
    db = mongo_client[get_mongo_db()]

    connection = pika.BlockingConnection(get_pika_params())
    thread = threading.Thread(target=start_listener)
    thread.setDaemon(True)
    thread.start()
    thread2 = threading.Thread(target=start_listener_data)
    thread2.setDaemon(True)
    thread2.start()
    app.run(host='0.0.0.0', port=int(port))
