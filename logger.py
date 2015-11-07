from flask import Flask, request
import os
import pika
import json
import threading

app = Flask(__name__)


@app.route("/ping")
def hello():
    return "pong"


def get_pika_params():
    if 'VCAP_SERVICES' in os.environ:
        vcap_service = json.loads(os.environ['VCAP_SERVICES'])

        return pika.URLParameters(url=vcap_service['rabbitmq'][0]['credentials']['uri'])

    return pika.ConnectionParameters(host="localhost")


def receive_new_message(ch, method, properties, body):
    data = json.loads(body)
    print("### %r" % ())
    print("EUI: %s %s - %s: %s %r" % (data['DevEUI_uplink']['DevEUI'],
                                      data['DevEUI_uplink']['Time'],
                                      data['DevEUI_uplink']['FPort'],
                                      data['DevEUI_uplink']['payload_hex'],
                                      ""
                                      ))


def start_listener():
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='data_log',
                       queue=queue_name)

    print('listener started')

    channel.basic_consume(receive_new_message,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


port = os.getenv('VCAP_APP_PORT', '5000')

connection = pika.BlockingConnection(get_pika_params())
channel = connection.channel()
thread = threading.Thread(target=start_listener)
thread.setDaemon(True)
thread.start()
channel.exchange_declare(exchange='data_log', type='fanout')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port), debug=True)
