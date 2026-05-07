import os
import threading
import time
import uuid
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

print("=" * 60)
print("🚀 INICIANDO SISTEMA RPC CON RABBITMQ")
print(f"   Host: {RABBITMQ_HOST}")
print(f"   VHost: {RABBITMQ_VHOST}")
print(f"   Cola: {RPC_QUEUE}")
print("=" * 60)

# Variables globales
responses = {}
responses_lock = threading.Lock()
worker_ready = False

# ==================== SERVIDOR RPC (WORKER INTERNO) ====================
def start_rpc_server():
    """Inicia el servidor RPC en un hilo separado"""
    global worker_ready
    
    def server_loop():
        global worker_ready
        try:
            print("[RPC Server] 🔌 Conectando a CloudAMQP...")
            
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=60,
                blocked_connection_timeout=300
            )
            
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            
            # Declarar cola
            channel.queue_declare(queue=RPC_QUEUE, durable=False)
            channel.basic_qos(prefetch_count=1)
            
            print(f"[RPC Server] ✅ Conectado exitosamente")
            print(f"[RPC Server] 🎧 Escuchando en cola: {RPC_QUEUE}")
            worker_ready = True
            
            def on_request(ch, method, props, body):
                mensaje = body.decode('utf-8')
                print(f"[RPC Server] 📩 Recibido: {mensaje}")
                
                # Procesar mensaje
                respuesta = f"✅ Respuesta RPC: '{mensaje}' recibido correctamente"
                
                # Enviar respuesta
                ch.basic_publish(
                    exchange='',
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(correlation_id=props.correlation_id),
                    body=respuesta
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"[RPC Server] 📤 Respuesta enviada")
            
            channel.basic_consume(queue=RPC_QUEUE, on_message_callback=on_request)
            channel.start_consuming()
            
        except Exception as e:
            print(f"[RPC Server] ❌ Error: {e}")
            worker_ready = False
    
    thread = threading.Thread(target=server_loop)
    thread.daemon = True
    thread.start()
    
    # Esperar a que el servidor esté listo
    time.sleep(5)
    print(f"[RPC Server] Estado: {'✅ Listo' if worker_ready else '❌ Falló'}")

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
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
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
            
            thread = threading.Thread(target=self._consume)
            thread.daemon = True
            thread.start()
            
            print("[RPC Client] ✅ Conectado a CloudAMQP")
        except Exception as e:
            print(f"[RPC Client] ❌ Error de conexión: {e}")
    
    def _consume(self):
        try:
            self.channel.start_consuming()
        except Exception as e:
            print(f"[RPC Client] Error en consumo: {e}")
    
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


# Inicializar componentes
print("\n[App] 1. Iniciando Servidor RPC...")
start_rpc_server()

print("[App] 2. Iniciando Cliente RPC...")
rpc_client = RpcClient()

print("[App] ✅ Sistema listo para recibir peticiones\n")


@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '')
        if mensaje:
            print(f"\n[Flask] 📝 Nuevo mensaje: {mensaje}")
            
            if not worker_ready:
                respuesta = "❌ El servidor RPC no está conectado. Revisa los logs."
            else:
                corr_id = rpc_client.send_request(mensaje)
                respuesta = rpc_client.get_response(corr_id, timeout=15)
                
                if respuesta is None:
                    respuesta = "⏰ Timeout: El servidor RPC no respondió"
                else:
                    print(f"[Flask] ✅ Respuesta enviada al usuario: {respuesta}")
    
    return render_template('index.html', respuesta=respuesta)


@app.route('/health')
def health():
    return {"status": "ok", "rpc_server_ready": worker_ready}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)