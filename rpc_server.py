import os
import amqpstorm
from amqpstorm import Message

def on_request(message):
    """Procesar solicitudes RPC."""
    try:
        print(f"[Servidor RPC] Recibido: {message.body}")
        
        # Procesar mensaje
        response = f"✅ Respuesta del servidor: '{message.body}' recibido correctamente"
        
        # Crear respuesta
        response_message = Message.create(message.channel, response)
        response_message.correlation_id = message.correlation_id
        response_message.publish(routing_key=message.reply_to)
        
        print(f"[Servidor RPC] Respuesta enviada a: {message.reply_to}")
        message.ack()
    except Exception as e:
        print(f"[Servidor RPC] Error: {e}")

def main():
    # Leer variables de entorno
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    username = os.environ.get('RABBITMQ_USER', 'guest')
    password = os.environ.get('RABBITMQ_PASS', 'guest')
    vhost = os.environ.get('RABBITMQ_VHOST', '/')
    queue = os.environ.get('RPC_QUEUE', 'rpc_queue')
    
    try:
        connection = amqpstorm.Connection(host, username, password, virtual_host=vhost)
        channel = connection.channel()
        channel.queue.declare(queue=queue, durable=True)
        
        print(f"[Servidor RPC] Esperando solicitudes en cola: {queue}")
        print(f"[Servidor RPC] Conectado a: {host} - VHost: {vhost}")
        
        channel.basic.consume(on_request, queue=queue)
        channel.start_consuming()
    except Exception as e:
        print(f"[Servidor RPC] Error de conexión: {e}")

if __name__ == "__main__":
    main()