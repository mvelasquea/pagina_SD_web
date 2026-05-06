import os
import threading
from time import sleep
from flask import Blueprint
import amqpstorm
from amqpstorm import Message

amqpstorm_bp = Blueprint('amqpstorm', __name__, url_prefix='/amqpstorm')

class RpcClientAmqpstorm(object):
    def __init__(self, host, username, password, vhost, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.vhost = vhost
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        self.connection = amqpstorm.Connection(
            self.host, self.username, self.password,
            virtual_host=self.vhost
        )
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

# Configuración desde variables de entorno
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

rpc_client_amqpstorm = RpcClientAmqpstorm(
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_VHOST, RPC_QUEUE
)

@amqpstorm_bp.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = rpc_client_amqpstorm.send_request(payload)
    while rpc_client_amqpstorm.queue[corr_id] is None:
        sleep(0.1)
    return rpc_client_amqpstorm.queue[corr_id]

@amqpstorm_bp.route('/')
def home():
    return "AMQPStorm RPC Client is running!"