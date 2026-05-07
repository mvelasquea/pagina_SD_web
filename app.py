import os
import time
import uuid
from flask import Flask, render_template, request

app = Flask(__name__)

# Almacenamiento temporal de solicitudes
pending_requests = {}
responses = {}

@app.route('/', methods=['GET', 'POST'])
def index():
    respuesta = None
    
    if request.method == 'POST':
        mensaje = request.form.get('mensaje', '')
        if mensaje:
            print(f"[Flask] Mensaje recibido: {mensaje}")
            
            # Generar ID único
            request_id = str(uuid.uuid4())
            
            # Simular procesamiento del servidor
            time.sleep(0.5)
            
            # Generar respuesta
            respuesta = f"Hola, recibí tu mensaje: '{mensaje}'"
            print(f"[Flask] Respuesta generada: {respuesta}")
    
    return render_template('index.html', respuesta=respuesta)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)