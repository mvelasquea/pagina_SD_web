import os
import uuid
import threading
import time
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

# Cola para respuestas
RESPONSE_QUEUE = 'rpc_responses'

# Variable global para almacenar respuestas
responses = {}
responses_lock = threading.Lock()

# ==================== SERVIDOR RPC (WORKER) ====================
class RpcWorker:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = False
        
    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run)
        thread.daemon = True
        thread.start()
        print("[Worker] ✅ Iniciado")
        
    def _run(self):
        try:
            # Conectar a RabbitMQ
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials
            )
            
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            # Declarar colas
            self.channel.queue_declare(queue=RPC_QUEUE, durable=False)
            self.channel.queue_declare(queue=RESPONSE_QUEUE, durable=False)
            
            print(f"[Worker] ✅ Conectado a CloudAMQP")
            print(f"[Worker] 📡 Escuchando en cola: {RPC_QUEUE}")
            
            # Consumir mensajes
            self.channel.basic_consume(
                queue=RPC_QUEUE,
                on_message_callback=self._process_request,
                auto_ack=False
            )
            
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"[Worker] ❌ Error: {e}")
    
    def _process_request(self, ch, method, props, body):
        mensaje = body.decode('utf-8')
        print(f"[Worker] 📩 Recibido: {mensaje}")
        
        # Procesar respuesta
        respuesta = f"✅ Respuesta: '{mensaje}' recibido correctamente"
        
        # Enviar respuesta a la cola de respuestas
        ch.basic_publish(
            exchange='',
            routing_key=RESPONSE_QUEUE,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=respuesta
        )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[Worker] 📤 Respuesta enviada")


# ==================== CLIENTE RPC ====================
class RpcClient:
    def __init__(self):
        self.connection = None
        self.channel = None
        
        # Conectar a RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials
        )
        
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        
        # Declarar colas
        self.channel.queue_declare(queue=RPC_QUEUE, durable=False)
        self.channel.queue_declare(queue=RESPONSE_QUEUE, durable=False)
        
        # Consumir respuestas
        self.channel.basic_consume(
            queue=RESPONSE_QUEUE,
            on_message_callback=self._on_response,
            auto_ack=True
        )
        
        # Iniciar hilo para consumir
        thread = threading.Thread(target=self._consume)
        thread.daemon = True
        thread.start()
        
        print("[Cliente] ✅ Conectado")
    
    def _consume(self):
        self.channel.start_consuming()
    
    def _on_response(self, ch, method, props, body):
        with responses_lock:
            responses[props.correlation_id] = body.decode('utf-8')
            print(f"[Cliente] ✅ Respuesta guardada para {props.correlation_id}")
    
    def send_request(self, mensaje):
        corr_id = str(uuid.uuid4())
        
        with responses_lock:
            responses[corr_id] = None
        
        self.channel.basic_publish(
            exchange='',
            routing_key=RPC_QUEUE,
            properties=pika.BasicProperties(correlation_id=corr_id),
            body=mensaje
        )
        
        print(f"[Cliente] 📤 Enviado: {mensaje} (ID: {corr_id})")
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
print("🚀 Iniciando Sistema RPC Distribuido")
print("=" * 50)

# Iniciar Worker
print("[App] 1. Iniciando Worker...")
worker = RpcWorker()
worker.start()

# Esperar conexión
time.sleep(3)

# Iniciar Cliente
print("[App] 2. Iniciando Cliente...")
cliente = RpcClient()

print("[App] ✅ Sistema listo!")


# ==================== FLASK ROUTES ====================
@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '')
        if mensaje:
            print(f"[Flask] 📝 Procesando: {mensaje}")
            
            # Enviar solicitud
            corr_id = cliente.send_request(mensaje)
            
            # Esperar respuesta
            respuesta = cliente.get_response(corr_id, timeout=10)
            
            if respuesta is None:
                respuesta = "⏰ Timeout: El servidor RPC no respondió"
            else:
                print(f"[Flask] ✅ Respuesta: {respuesta}")
    
    return render_template('index.html', respuesta=respuesta)


@app.route('/health')
def health():
    return {"status": "ok"}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)