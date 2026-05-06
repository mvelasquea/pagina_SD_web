import os
import uuid
import threading
import time
from flask import Flask, render_template, request
import pika

app = Flask(__name__)

# Configuración de CloudAMQP
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
RPC_QUEUE = os.environ.get('RPC_QUEUE', 'rpc_queue')

class RpcServer:
    """Servidor RPC usando Pika - corre en un hilo separado"""
    
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = False
        
    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run)
        thread.daemon = True
        thread.start()
        print("[Worker] ✅ Hilo del servidor iniciado")
        
    def _run(self):
        try:
            # Configurar conexión
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            params = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                virtual_host=RABBITMQ_VHOST,
                credentials=credentials,
                heartbeat=60
            )
            
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            # Declarar cola
            self.channel.queue_declare(queue=RPC_QUEUE, durable=False)
            
            print(f"[Worker] ✅ Conectado a CloudAMQP. Escuchando en '{RPC_QUEUE}'")
            
            # Consumir mensajes
            self.channel.basic_consume(
                queue=RPC_QUEUE,
                on_message_callback=self._on_request,
                auto_ack=False
            )
            self.channel.start_consuming()
            
        except Exception as e:
            print(f"[Worker] ❌ Error: {e}")
            self.running = False
    
    def _on_request(self, ch, method, props, body):
        """Procesar solicitud RPC"""
        mensaje = body.decode('utf-8')
        print(f"[Worker] 📩 Recibido: {mensaje}")
        
        # Generar respuesta
        respuesta = f"✅ Respuesta del servidor: '{mensaje}' recibido correctamente"
        
        # Enviar respuesta a la cola de callback
        ch.basic_publish(
            exchange='',
            routing_key=props.reply_to,
            properties=pika.BasicProperties(correlation_id=props.correlation_id),
            body=respuesta
        )
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[Worker] 📤 Respuesta enviada")


class RpcClient:
    """Cliente RPC usando Pika"""
    
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.responses = {}
        self.lock = threading.Lock()
        
        # Conectar
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=60
        )
        
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        
        # Crear cola de callback exclusiva
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        # Consumir respuestas
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self._on_response,
            auto_ack=True
        )
        
        # Iniciar hilo para procesar eventos
        thread = threading.Thread(target=self._process_events)
        thread.daemon = True
        thread.start()
        
        print("[Cliente] ✅ Conectado a CloudAMQP")
    
    def _process_events(self):
        self.channel.start_consuming()
    
    def _on_response(self, ch, method, props, body):
        with self.lock:
            self.responses[props.correlation_id] = body.decode('utf-8')
    
    def send_request(self, mensaje):
        corr_id = str(uuid.uuid4())
        
        with self.lock:
            self.responses[corr_id] = None
        
        self.channel.basic_publish(
            exchange='',
            routing_key=RPC_QUEUE,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr_id,
            ),
            body=mensaje
        )
        
        print(f"[Cliente] 📤 Mensaje enviado: {mensaje} (ID: {corr_id})")
        return corr_id
    
    def get_response(self, corr_id, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                response = self.responses.get(corr_id)
                if response is not None:
                    del self.responses[corr_id]
                    return response
            time.sleep(0.1)
        return None


# Inicializar servicios
print("=" * 50)
print("🚀 Iniciando Sistema RPC Distribuido")
print("=" * 50)

print("[App] 1. Iniciando Worker (Servidor RPC)...")
worker = RpcServer()
worker.start()

# Esperar a que el worker se conecte
time.sleep(3)

print("[App] 2. Iniciando Cliente RPC...")
cliente = RpcClient()

print("[App] ✅ Sistema listo!")


@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '')
        if mensaje:
            print(f"[Flask] Enviando: {mensaje}")
            
            # Enviar solicitud RPC
            corr_id = cliente.send_request(mensaje)
            
            # Esperar respuesta
            respuesta = cliente.get_response(corr_id, timeout=10)
            
            if respuesta is None:
                respuesta = "⏰ Timeout: El servidor RPC no respondió"
            else:
                print(f"[Flask] Respuesta recibida: {respuesta}")
    
    return render_template('index.html', respuesta=respuesta)


@app.route('/health')
def health():
    return {"status": "ok", "worker_running": worker.running}


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)