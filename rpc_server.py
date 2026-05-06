import os
import amqpstorm
from amqpstorm import Message

def on_request(message):
    """Procesar solicitudes RPC."""
    try:
        print(f"[Servidor RPC] 📩 Recibido: {message.body}")
        
        # Procesar respuesta
        response = f"✅ Respuesta del servidor: '{message.body}' recibido correctamente"
        
        # Crear respuesta
        response_message = Message.create(message.channel, response)
        response_message.correlation_id = message.correlation_id
        response_message.publish(routing_key=message.reply_to)
        
        print(f"[Servidor RPC] 📤 Respuesta enviada a: {message.reply_to}")
        message.ack()
        
    except Exception as e:
        print(f"[Servidor RPC] ❌ Error procesando: {e}")

def main():
    # Leer variables de entorno (CloudAMQP)
    host = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
    username = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
    password = os.environ.get('RABBITMQ_PASS')
    vhost = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')
    queue = os.environ.get('RPC_QUEUE', 'rpc_queue')
    
    print(f"[Servidor RPC] 🚀 Iniciando...")
    print(f"   Host: {host}")
    print(f"   Usuario: {username}")
    print(f"   VHost: {vhost}")
    print(f"   Cola: {queue}")
    
    try:
        connection = amqpstorm.Connection(
            host, 
            username, 
            password, 
            virtual_host=vhost
        )
        
        channel = connection.channel()
        
        # Cola RPC principal
        channel.queue.declare(queue=queue, durable=False)
        
        print(f"[Servidor RPC] ✅ Conectado a CloudAMQP")
        print(f"[Servidor RPC] 🎧 Esperando solicitudes en cola: {queue}")
        
        channel.basic_consume(on_request, queue=queue)
        channel.start_consuming()
        
    except Exception as e:
        print(f"[Servidor RPC] ❌ Error de conexión: {e}")

if __name__ == "__main__":
    main()