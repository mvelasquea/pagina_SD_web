import os
import threading
from time import sleep
from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient(object):
    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        # Usar host, username, password en lugar de URL
        self.connection = amqpstorm.Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id

# Leer configuración desde variables de entorno
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

# Crear el cliente RPC con host, user, pass
RPC_CLIENT = RpcClient(RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RPC_QUEUE)

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = RPC_CLIENT.send_request(payload)
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)
    return RPC_CLIENT.queue[corr_id]

@app.route('/')
def home():
    return "RPC Client is running!"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)