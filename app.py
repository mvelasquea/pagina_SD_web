import os
import ssl
import time
import uuid
import threading
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración con valores por defecto (como originalmente)
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')
AMQPS_PORT = 5671

print("=" * 60)
print("🚀 SISTEMA RPC CON RABBITMQ + SSL")
print(f"   Host: {RABBITMQ_HOST}:{AMQPS_PORT}")
print(f"   VHost: {RABBITMQ_VHOST}   Cola: {RPC_QUEUE}")
print("=" * 60)

worker_ready = False

# Contexto SSL
ssl_context = ssl.create_default_context()
ssl_options = pika.SSLOptions(ssl_context, server_hostname=RABBITMQ_HOST)

# ==================== SERVIDOR RPC ====================
def start_rpc_server():
    global worker_ready

    def server_loop():
        global worker_ready
        while True:
            try:
                print("[RPC Server] 🔌 Conectando...")
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
                conn = pika.BlockingConnection(params)
                ch = conn.channel()
                ch.queue_declare(queue=RPC_QUEUE, durable=False)
                ch.basic_qos(prefetch_count=1)
                worker_ready = True
                print(f"[RPC Server] ✅ Listo y escuchando en '{RPC_QUEUE}'")

                def on_request(ch, method, props, body):
                    mensaje = body.decode('utf-8')
                    print(f"[RPC Server] 📩 {mensaje}")
                    respuesta = f"✅ RPC: '{mensaje}' procesado"
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=pika.BasicProperties(correlation_id=props.correlation_id),
                        body=respuesta
                    )
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    print("[RPC Server] 📤 Respuesta enviada")

                ch.basic_consume(queue=RPC_QUEUE, on_message_callback=on_request)
                ch.start_consuming()

            except pika.exceptions.AMQPConnectionError as e:
                print(f"[RPC Server] ❌ Error conexión: {e}")
                worker_ready = False
                time.sleep(5)
            except Exception as e:
                print(f"[RPC Server] ❌ Error inesperado: {e}")
                worker_ready = False
                time.sleep(5)

    thread = threading.Thread(target=server_loop, daemon=True)
    thread.start()
    time.sleep(4)

# ==================== CLIENTE RPC (SÍNCRONO, SIN HILOS) ====================
class RpcClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self._connect()

    def _connect(self):
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
        print("[RPC Client] ✅ Conectado (SSL)")

    def call(self, message, timeout=15):
        """
        Envía una solicitud RPC y espera la respuesta de forma síncrona.
        """
        corr_id = str(uuid.uuid4())
        response = None

        def on_response(ch, method, props, body):
            nonlocal response
            if props.correlation_id == corr_id:
                response = body.decode('utf-8')
                ch.basic_cancel(consumer_tag)

        consumer_tag = self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=on_response,
            auto_ack=True
        )

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

        deadline = time.time() + timeout
        while response is None and time.time() < deadline:
            self.connection.process_data_events(time_limit=1)

        try:
            self.channel.basic_cancel(consumer_tag)
        except Exception:
            pass

        if response is None:
            print(f"[RPC Client] ⏰ Timeout para {corr_id}")
        else:
            print(f"[RPC Client] ✅ Respuesta: {response}")
        return response

# ==================== INICIALIZACIÓN ====================
print("\n[App] 1. Iniciando servidor RPC...")
start_rpc_server()

print("[App] 2. Iniciando cliente RPC...")
try:
    rpc_client = RpcClient()
except Exception as e:
    print(f"[App] ❌ No se pudo iniciar el cliente: {e}")
    rpc_client = None

print("[App] ✅ Sistema listo\n")

@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '').strip()
        if mensaje:
            print(f"[Flask] 📝 Nuevo mensaje: {mensaje}")
            if not worker_ready or rpc_client is None:
                respuesta = "❌ El sistema RPC no está disponible. Revisa los logs."
            else:
                respuesta = rpc_client.call(mensaje)
                if respuesta is None:
                    respuesta = "⏰ Timeout: El servidor RPC no respondió"
    return render_template('index.html', respuesta=respuesta)

@app.route('/health')
def health():
    return {
        "status": "ok",
        "rpc_server_ready": worker_ready,
        "rpc_client": rpc_client is not None
    }

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)