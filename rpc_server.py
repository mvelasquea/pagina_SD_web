import os
import amqpstorm
from amqpstorm import Message


def on_request(message):
    """Procesar solicitudes RPC."""
    try:
        print(f"[Servidor RPC] Recibido: {message.body}")

        response = f"Hola, recibí tu mensaje: {message.body}"

        response_message = Message.create(
            message.channel,
            response
        )

        response_message.correlation_id = message.correlation_id
        response_message.reply_to = message.reply_to

        response_message.publish(
            routing_key=message.reply_to
        )

        print(f"[Servidor RPC] Respuesta enviada.")
        message.ack()

    except Exception as e:
        print(f"[Servidor RPC] Error procesando solicitud: {e}")


def main():
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rat.rmq2.cloudamqp.com')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'ssxppfqn')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'fUxvCQey_0uAHrCbvTVTCvFicYLbm3eN')
    rabbitmq_vhost = os.environ.get('RABBITMQ_VHOST', 'ssxppfqn')  # ← CLAVE
    rabbitmq_queue = os.environ.get('RPC_QUEUE', 'rpc_queue')

    try:
        # IMPORTANTE: agregar virtual_host
        connection = amqpstorm.Connection(
            rabbitmq_host,
            rabbitmq_user,
            rabbitmq_pass,
            virtual_host=rabbitmq_vhost  # ← CLAVE
        )

        channel = connection.channel()

        channel.queue.declare(
            queue=rabbitmq_queue,
            durable=True
        )

        print(f"[Servidor RPC] Esperando solicitudes en cola: {rabbitmq_queue}")
        print(f"[Servidor RPC] Conectado a vhost: {rabbitmq_vhost}")

        channel.basic_consume(
            on_request,
            queue=rabbitmq_queue
        )

        channel.start_consuming()

    except Exception as e:
        print(f"[Servidor RPC] Error de conexión: {e}")


if __name__ == "__main__":
    main()