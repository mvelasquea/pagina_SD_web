import os
import ssl
import threading
import time
import uuid
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración desde variables de entorno (sin valores por defecto, por seguridad)
RABBITMQ_HOST = os.environ['RABBITMQ_HOST']
RABBITMQ_USER = os.environ['RABBITMQ_USER']
RABBITMQ_PASS = os.environ['RABBITMQ_PASS']
RABBITMQ_VHOST = os.environ['RABBITMQ_VHOST']
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

# Puerto AMQPS estándar de CloudAMQP
AMQPS_PORT = 5671

print("=" * 60)
print("🚀 INICIANDO SISTEMA RPC CON RABBITMQ + SSL")
print(f"   Host: {RABBITMQ_HOST}")
print(f"   Puerto: {AMQPS_PORT}")
print(f"   VHost: {RABBITMQ_VHOST}")
print(f"   Cola: {RPC_QUEUE}")
print("=" * 60)

# Variables globales
responses = {}
responses_lock = threading.Lock()
worker_ready = False

# Contexto SSL para todas las conexiones
ssl_context = ssl.create_default_context()
ssl_options = pika.SSLOptions(ssl_context, server_hostname=RABBITMQ_HOST)

# ==================== SERVIDOR RPC (WORKER INTERNO) ====================
def start_rpc_server():
    """Inicia el servidor RPC en un hilo con reconexión automática"""
    global worker_ready

    def server_loop():
        global worker_ready
        while True:
            try:
                print("[RPC Server] 🔌 Intentando conectar a CloudAMQP (SSL)...")
                credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
                params = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=AMQPS_PORT,
                    virtual_host=RABBITMQ_VHOST,
                    credentials=credentials,
                    ssl_options=ssl_options,
                    heartbeat=60,
                    blocked_connection_timeout=300
                )

                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                channel.queue_declare(queue=RPC_QUEUE, durable=False)
                channel.basic_qos(prefetch_count=1)

                print(f"[RPC Server] ✅ Conectado y escuchando en {RPC_QUEUE}")
                worker_ready = True

                def on_request(ch, method, props, body):
                    mensaje = body.decode('utf-8')
                    print(f"[RPC Server] 📩 Recibido: {mensaje}")
                    respuesta = f"✅ Respuesta RPC: '{mensaje}' recibido correctamente"
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id=props.correlation_id),
                        body=respuesta
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("[RPC Server] 📤 Respuesta enviada")

                channel.basic_consume(queue=RPC_QUEUE, on_message_callback=on_request)
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as e:
                print(f"[RPC Server] ❌ Error de conexión: {e}")
                worker_ready = False
                print("[RPC Server] 🔄 Reintentando en 5 segundos...")
                time.sleep(5)
            except Exception as e:
                print(f"[RPC Server] ❌ Error inesperado: {e}")
                worker_ready = False
                time.sleep(5)

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()
    time.sleep(5)  # pequeño margen para la primera conexión

# ==================== CLIENTE RPC ====================
class RpcClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self._connect()

    def _connect(self):
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=AMQPS_PORT,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                ssl_options=ssl_options,
                heartbeat=60
            )
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=RPC_QUEUE, durable=False)

            result = self.channel.queue_declare(queue='', exclusive=True)
            self.callback_queue = result.method.queue

            self.channel.basic_consume(
                queue=self.callback_queue,
                on_message_callback=self._on_response,
                auto_ack=True
            )

            # Hilo de consumo
            self.thread = threading.Thread(target=self._consume, daemon=True)
            self.thread.start()

            print("[RPC Client] ✅ Conectado a CloudAMQP (SSL)")
        except Exception as e:
            print(f"[RPC Client] ❌ Error de conexión: {e}")
            raise

    def _consume(self):
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(f"[RPC Client] Consumo detenido: {e}")

    def _on_response(self, ch, method, props, body):
        with responses_lock:
            responses[props.correlation_id] = body.decode('utf-8')
            print(f"[RPC Client] ✅ Respuesta recibida para {props.correlation_id}")

    def send_request(self, message):
        corr_id = str(uuid.uuid4())
        with responses_lock:
            responses[corr_id] = None

        self.channel.basic_publish(
            exchange='',
            routing_key=RPC_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr_id,
            ),
            body=message
        )
        print(f"[RPC Client] 📤 Enviado: '{message}' (ID: {corr_id})")
        return corr_id

    def get_response(self, corr_id, timeout=15):
        start = time.time()
        while time.time() - start < timeout:
            with responses_lock:
                response = responses.get(corr_id)
                if response is not None:
                    del responses[corr_id]
                    return response
            time.sleep(0.2)
        print(f"[RPC Client] ⏰ Timeout para {corr_id}")
        return None

# ==================== INICIALIZACIÓN ====================
print("\n[App] Iniciando servidor RPC...")
start_rpc_server()

print("[App] Iniciando cliente RPC...")
try:
    rpc_client = RpcClient()
except Exception:
    print("[App] ❌ No se pudo iniciar el cliente RPC")
    rpc_client = None

print("[App] ✅ Sistema listo\n")

@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '').strip()
        if mensaje:
            print(f"\n[Flask] 📝 Mensaje: {mensaje}")
            if not worker_ready or rpc_client is None:
                respuesta = "❌ El sistema RPC no está disponible. Revisa los logs."
            else:
                corr_id = rpc_client.send_request(mensaje)
                respuesta = rpc_client.get_response(corr_id)
                if respuesta is None:
                    respuesta = "⏰ Timeout: El servidor RPC no respondió"
                else:
                    print(f"[Flask] ✅ Respuesta para el usuario: {respuesta}")
    return render_template('index.html', respuesta=respuesta)

@app.route('/health')
def health():
    return {"status": "ok", "rpc_server_ready": worker_ready, "rpc_client": rpc_client is not None}

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)