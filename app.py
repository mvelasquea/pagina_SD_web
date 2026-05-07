import os
import threading
from time import sleep
from flask import Flask, render_template, request
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient(object):
    """Asynchronous RPC Client."""

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

        try:
            self.open()
            print("[Cliente RPC] Conexión establecida correctamente.")
        except Exception as e:
            print(f"[Cliente RPC] Error al conectar: {e}")

    def open(self):
        """Open RabbitMQ connection."""
        self.connection = amqpstorm.Connection(
            self.host,
            self.username,
            self.password,
            virtual_host=self.vhost
        )

        self.channel = self.connection.channel()

        # Cola principal RPC - durable=False para CloudAMQP
        self.channel.queue.declare(
            queue=self.rpc_queue,
            durable=False  # ← CAMBIADO de True a False
        )

        # Cola exclusiva de callback
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        # Consumidor de respuestas
        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )

        self._create_process_thread()

    def _create_process_thread(self):
        """Create thread for consuming RPC responses."""
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        """Consume incoming responses."""
        try:
            self.channel.start_consuming(to_tuple=False)
        except Exception as e:
            print(f"[Cliente RPC] Error consumiendo mensajes: {e}")

    def _on_response(self, message):
        """Callback executed when a response arrives."""
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        """Send RPC request."""
        try:
            message = Message.create(self.channel, payload)
            message.reply_to = self.callback_queue
            self.queue[message.correlation_id] = None
            message.publish(routing_key=self.rpc_queue)
            return message.correlation_id
        except Exception as e:
            print(f"[Cliente RPC] Error enviando solicitud: {e}")
            return None

    def has_response(self, correlation_id):
        """Check if response exists."""
        return self.queue.get(correlation_id) is not None

    def get_response(self, correlation_id):
        """Retrieve and clean response."""
        response = self.queue.get(correlation_id)
        if correlation_id in self.queue:
            del self.queue[correlation_id]
        return response


# Configuración desde variables de entorno
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

# Crear el cliente RPC
RPC_CLIENT = RpcClient(
    RABBITMQ_HOST,
    RABBITMQ_USER,
    RABBITMQ_PASS,
    RABBITMQ_VHOST,
    RPC_QUEUE
)


@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None

    if request.method == 'POST':
        try:
            mensaje = request.form['mensaje']
            print(f"[Flask] Enviando: {mensaje}")
            corr_id = RPC_CLIENT.send_request(mensaje)

            if corr_id is None:
                respuesta = "No se pudo enviar la solicitud RPC."
            else:
                timeout = 10
                elapsed = 0
                while not RPC_CLIENT.has_response(corr_id):
                    sleep(0.1)
                    elapsed += 0.1
                    if elapsed >= timeout:
                        respuesta = "Timeout: el servidor RPC no respondió."
                        break
                else:
                    respuesta = RPC_CLIENT.get_response(corr_id)
                    print(f"[Flask] Respuesta: {respuesta}")
        except Exception as e:
            respuesta = f"Error de conexión RPC: {str(e)}"

    return render_template('index.html', respuesta=respuesta)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)