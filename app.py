import os
import threading
import time
import uuid
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración de CloudAMQP
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

# Variable global para respuestas
responses = {}
responses_lock = threading.Lock()
worker_running = False

# ==================== SERVIDOR RPC (WORKER EN HILO) ====================
def start_worker():
    """Inicia el servidor RPC en un hilo separado"""
    global worker_running
    
    def worker_thread():
        global worker_running
        try:
            print("[Worker] 🚀 Iniciando...")
            
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=60
            )
            
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue=RPC_QUEUE, durable=False)
            
            print(f"[Worker] ✅ Conectado. Escuchando en: {RPC_QUEUE}")
            worker_running = True
            
            def on_request(ch, method, props, body):
                mensaje = body.decode('utf-8')
                print(f"[Worker] 📩 Recibido: {mensaje}")
                
                respuesta = f"Hola, recibí tu mensaje: {mensaje}"
                
                ch.basic_publish(
                    exchange='',
                    routing_key=props.reply_to,
                    properties=pika.BasicProperties(correlation_id=props.correlation_id),
                    body=respuesta
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)
                print(f"[Worker] 📤 Respondido: {respuesta}")
            
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RPC_QUEUE, on_message_callback=on_request)
            channel.start_consuming()
            
        except Exception as e:
            print(f"[Worker] ❌ Error: {e}")
            worker_running = False
    
    thread = threading.Thread(target=worker_thread)
    thread.daemon = True
    thread.start()
    time.sleep(3)  # Esperar a que el worker se conecte


# ==================== CLIENTE RPC ====================
class RpcClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.connect()
    
    def connect(self):
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
            on_message_callback=self.on_response,
            auto_ack=True
        )
        
        thread = threading.Thread(target=self._consume)
        thread.daemon = True
        thread.start()
        print("[Cliente] ✅ Conectado")
    
    def _consume(self):
        self.channel.start_consuming()
    
    def on_response(self, ch, method, props, body):
        with responses_lock:
            responses[props.correlation_id] = body.decode('utf-8')
    
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
        print(f"[Cliente] 📤 Enviado: {message}")
        return corr_id
    
    def get_response(self, corr_id, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            with responses_lock:
                response = responses.get(corr_id)
                if response is not None:
                    del responses[corr_id]
                    return response
            time.sleep(0.1)
        return None


# ==================== INICIALIZACIÓN ====================
print("=" * 50)
print("🚀 Iniciando Sistema RPC")
print("=" * 50)

# Iniciar worker en hilo
print("[App] Iniciando Worker interno...")
start_worker()

# Iniciar cliente
print("[App] Iniciando Cliente...")
rpc_client = RpcClient()

print("[App] ✅ Sistema listo!")


# ==================== RUTAS FLASK ====================
@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '')
        if mensaje:
            print(f"[Flask] 📝 Procesando: {mensaje}")
            corr_id = rpc_client.send_request(mensaje)
            respuesta = rpc_client.get_response(corr_id, timeout=10)
            
            if respuesta is None:
                respuesta = "⏰ Timeout: El servidor RPC no respondió"
    
    return render_template('index.html', respuesta=respuesta)


@app.route('/health')
def health():
    return {"status": "ok", "worker_running": worker_running}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)