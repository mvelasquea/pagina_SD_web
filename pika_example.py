import os
import pika
import uuid
import threading
from time import sleep
from flask import Blueprint

pika_bp = Blueprint('pika', __name__, url_prefix='/pika')

class RpcClientPika(object):
    internal_lock = threading.Lock()
    queue = {}

    def __init__(self, host, username, password, vhost, rpc_queue):
        self.rpc_queue = rpc_queue
        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(
            host=host,
            virtual_host=vhost,
            credentials=credentials,
            heartbeat=600
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )
        while True:
            with self.internal_lock:
                self.connection.process_data_events()
                sleep(0.1)

    def _on_response(self, ch, method, props, body):
        self.queue[props.correlation_id] = body

    def send_request(self, payload):
        corr_id = str(uuid.uuid4())
        self.queue[corr_id] = None
        with self.internal_lock:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.rpc_queue,
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=corr_id,
                ),
                body=payload
            )
        return corr_id

# Configuración desde variables de entorno
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

rpc_client_pika = RpcClientPika(
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_VHOST, RPC_QUEUE
)

@pika_bp.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = rpc_client_pika.send_request(payload)
    while rpc_client_pika.queue[corr_id] is None:
        sleep(0.1)
    return rpc_client_pika.queue[corr_id]

@pika_bp.route('/')
def home():
    return "Pika RPC Client is running!"