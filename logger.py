import json
import os
import pprint
import threading

from dateutil.parser import parse
import pika
import pymongo
from flask import Flask
from pymongo import MongoClient

app = Flask(__name__)


@app.route("/ping")
def hello():
    return "pong"


@app.route("/avg")
def avg():
    return "nope"


@app.route("/sample")
def sample():
    cursor = db.full_data.find().sort([("Time", pymongo.DESCENDING)])
    element = cursor[0]
    pprint.pprint(element)
    return "%s - %s" % (element['payload_hex'], element['Time'])


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
