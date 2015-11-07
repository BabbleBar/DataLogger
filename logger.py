from flask import Flask, request
import os
import pika
import json
import threading

app = Flask(__name__)


last_log = 'no data'

@app.route("/")
def hello():
    return "Hello World!"


def get_pika_params():
    if 'VCAP_SERVICES' in os.environ:
        VCAP_SERVICES = json.loads(os.environ['VCAP_SERVICES'])

        return pika.URLParameters(url=VCAP_SERVICES['rabbitmq'][0]['credentials']['uri'])

    return pika.ConnectionParameters(host="localhost")


def start_listener():
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='data_log',
                       queue=queue_name)

    print(' [*] Waiting for logs. ')

    def callback(ch, method, properties, body):
        print(" [x] %r" % (body,))
        last_log = body

    channel.basic_consume(callback,
                          queue=queue_name,
                          no_ack=True)

    channel.start_consuming()


@app.route("/last")
def last():
    return last_log


port = os.getenv('VCAP_APP_PORT', '5000')


connection = pika.BlockingConnection(get_pika_params())
channel = connection.channel()
thread = threading.Thread(target=start_listener)
thread.setDaemon(True)
thread.start()
channel.exchange_declare(exchange='data_log', type='fanout')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(port), debug=True)
