import os
from flask import Flask, render_template_string

app = Flask(__name__)

# Importar los blueprints
from amqpstorm_example import amqpstorm_bp
from pika_example import pika_bp

# Registrar los blueprints
app.register_blueprint(amqpstorm_bp)
app.register_blueprint(pika_bp)

# HTML del menú principal
MENU_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>RPC Clients - RabbitMQ Examples</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 50px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        h1 {
            color: #333;
            text-align: center;
        }
        .card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .card h2 {
            margin-top: 0;
            color: #ff6600;
        }
        .card a {
            display: inline-block;
            background-color: #ff6600;
            color: white;
            padding: 10px 20px;
            text-decoration: none;
            border-radius: 4px;
            margin-top: 10px;
            margin-right: 10px;
        }
        .card a:hover {
            background-color: #cc5500;
        }
        .endpoint {
            background-color: #e9ecef;
            padding: 10px;
            border-radius: 4px;
            font-family: monospace;
            margin: 10px 0;
        }
        footer {
            text-align: center;
            margin-top: 30px;
            color: #666;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <h1>🐰 RabbitMQ RPC Clients</h1>
    <p style="text-align: center">Selecciona qué implementación probar:</p>
    
    <div class="card">
        <h2>📦 AMQPStorm</h2>
        <p>Cliente RPC asíncrono usando la librería <strong>amqpstorm</strong>.</p>
        <div class="endpoint">
            GET /amqpstorm/rpc_call/&lt;mensaje&gt;
        </div>
        <a href="/amqpstorm/">Ver estado</a>
        <a href="/amqpstorm/rpc_call/hola%20mundo">Probar con "hola mundo"</a>
    </div>
    
    <div class="card">
        <h2>🐍 Pika</h2>
        <p>Cliente RPC asíncrono usando la librería <strong>pika</strong>.</p>
        <div class="endpoint">
            GET /pika/rpc_call/&lt;mensaje&gt;
        </div>
        <a href="/pika/">Ver estado</a>
        <a href="/pika/rpc_call/hola%20mundo">Probar con "hola mundo"</a>
    </div>
    
    <footer>
        ⚠️ Necesitas un worker activo escuchando en la cola 'rpc_queue' para recibir respuestas.
    </footer>
</body>
</html>
'''

@app.route('/')
def home():
    return render_template_string(MENU_HTML)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)