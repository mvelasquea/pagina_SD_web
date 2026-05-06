import os
import threading
import time
from flask import Flask, render_template, request
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcServer:
    """Servidor RPC que corre en un hilo separado."""
    
    def __init__(self, host, username, password, vhost, queue_name):
        self.host = host
        self.username = username
        self.password = password
        self.vhost = vhost
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.running = False
    
    def start(self):
        """Iniciar el servidor RPC en un hilo."""
        self.running = True
        thread = threading.Thread(target=self._run)
        thread.daemon = True
        thread.start()
        print("[Servidor RPC] Hilo iniciado")
    
    def _run(self):
        """Ejecutar el servidor RPC."""
        try:
            print(f"[Servidor RPC] Conectando a {self.host}...")
            self.connection = amqpstorm.Connection(
                self.host,
                self.username,
                self.password,
                virtual_host=self.vhost
            )
            self.channel = self.connection.channel()
            
            # Cola RPC
            self.channel.queue.declare(queue=self.queue_name, durable=False)
            
            print(f"[Servidor RPC] ✅ Conectado exitosamente. Escuchando en cola: {self.queue_name}")
            
            # Consumir mensajes
            self.channel.basic.consume(self._on_request, queue=self.queue_name)
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"[Servidor RPC] ❌ Error de conexión: {e}")
            self.running = False
    
    def _on_request(self, message):
        """Procesar solicitud RPC."""
        try:
            print(f"[Servidor RPC] 📩 Recibido: {message.body}")
            
            # Procesar respuesta
            response = f"✅ Respuesta del servidor: '{message.body}' recibido correctamente"
            
            # Enviar respuesta
            self.channel.basic_publish(
                exchange='',
                routing_key=message.reply_to,
                body=response,
                properties={
                    'correlation_id': message.correlation_id
                }
            )
            
            print(f"[Servidor RPC] 📤 Respuesta enviada")
            message.ack()
            
        except Exception as e:
            print(f"[Servidor RPC] ❌ Error procesando: {e}")


class RpcClient(object):
    """Cliente RPC."""

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
            print("[Cliente RPC] ✅ Conexión establecida correctamente.")
        except Exception as e:
            print(f"[Cliente RPC] ❌ Error al conectar: {e}")

    def open(self):
        """Open RabbitMQ connection."""
        self.connection = amqpstorm.Connection(
            self.host,
            self.username,
            self.password,
            virtual_host=self.vhost
        )

        self.channel = self.connection.channel()
        self.channel.queue.declare(queue=self.rpc_queue, durable=False)
        
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )

        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        try:
            self.channel.start_consuming(to_tuple=False)
        except Exception as e:
            print(f"[Cliente RPC] Error consumiendo: {e}")

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body
        print(f"[Cliente RPC] Respuesta recibida: {message.body}")

    def send_request(self, payload):
        try:
            message = Message.create(self.channel, payload)
            message.reply_to = self.callback_queue
            self.queue[message.correlation_id] = None
            message.publish(routing_key=self.rpc_queue)
            print(f"[Cliente RPC] 📤 Mensaje enviado: {payload}")
            return message.correlation_id
        except Exception as e:
            print(f"[Cliente RPC] Error enviando: {e}")
            return None

    def has_response(self, correlation_id):
        return self.queue.get(correlation_id) is not None

    def get_response(self, correlation_id):
        response = self.queue.get(correlation_id)
        if correlation_id in self.queue:
            del self.queue[correlation_id]
        return response


# Configuración
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

print("=" * 50)
print("🚀 Iniciando aplicación...")
print("=" * 50)

# Iniciar el servidor RPC
print("[App] 1. Iniciando Servidor RPC...")
rpc_server = RpcServer(RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_VHOST, RPC_QUEUE)
rpc_server.start()

# Esperar conexión
time.sleep(3)

# Crear cliente RPC
print("[App] 2. Iniciando Cliente RPC...")
RPC_CLIENT = RpcClient(
    RABBITMQ_HOST, RABBITMQ_USER, RABBITMQ_PASS, RABBITMQ_VHOST, RPC_QUEUE
)

print("[App] ✅ Aplicación lista!")

@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None

    if request.method == 'POST':
        try:
            mensaje = request.form['mensaje']
            print(f"[Flask] Enviando: {mensaje}")
            
            corr_id = RPC_CLIENT.send_request(mensaje)

            if corr_id is None:
                respuesta = "❌ No se pudo enviar la solicitud RPC."
            else:
                timeout = 10
                elapsed = 0
                while not RPC_CLIENT.has_response(corr_id):
                    time.sleep(0.1)
                    elapsed += 0.1
                    if elapsed >= timeout:
                        respuesta = "⏰ Timeout: El servidor RPC no respondió"
                        break
                else:
                    respuesta = RPC_CLIENT.get_response(corr_id)
        except Exception as e:
            respuesta = f"❌ Error: {str(e)}"

    return render_template('index.html', respuesta=respuesta)

@app.route('/health')
def health():
    return {"status": "ok"}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)