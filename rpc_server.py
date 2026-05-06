import os
import amqpstorm
from amqpstorm import Message


def on_request(message):
    """Procesar solicitudes RPC."""
    try:
        print(f"[Servidor RPC] Recibido: {message.body}")

        # Procesamiento
        response = f"Hola, recibí tu mensaje: {message.body}"

        # Crear respuesta
        response_message = Message.create(
            message.channel,
            response
        )

        # Mantener relación request-response
        response_message.correlation_id = message.correlation_id

        # Cola callback del cliente
        response_message.reply_to = message.reply_to

        # Enviar respuesta
        response_message.publish(
            routing_key=message.reply_to
        )

        print(f"[Servidor RPC] Respuesta enviada.")

        # Confirmar mensaje procesado
        message.ack()

    except Exception as e:
        print(f"[Servidor RPC] Error procesando solicitud: {e}")


def main():
    # Leer variables de entorno para CloudAMQP
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'localhost')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'guest')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'guest')
    rabbitmq_queue = os.environ.get('RPC_QUEUE', 'rpc_queue')

    try:
        connection = amqpstorm.Connection(
            rabbitmq_host,
            rabbitmq_user,
            rabbitmq_pass
        )

        channel = connection.channel()

        # Cola RPC principal
        channel.queue.declare(
            queue=rabbitmq_queue,
            durable=True
        )

        print(f"[Servidor RPC] Esperando solicitudes en cola: {rabbitmq_queue}")

        channel.basic.consume(
            on_request,
            queue=rabbitmq_queue
        )

        channel.start_consuming()

    except Exception as e:
        print(f"[Servidor RPC] Error de conexión: {e}")


if __name__ == "__main__":
    main()