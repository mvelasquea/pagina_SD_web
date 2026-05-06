from flask import Flask, render_template_string, request, jsonify, session
import json
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'mi_clave_secreta_123')

# Menú de comidas
MENU = [
    {"id": 1, "nombre": "🍕 Pizza Pepperoni", "precio": 12.99, "categoria": "Pizzas"},
    {"id": 2, "nombre": "🍔 Hamburguesa Clásica", "precio": 8.99, "categoria": "Hamburguesas"},
    {"id": 3, "nombre": "🌮 Tacos al Pastor", "precio": 6.99, "categoria": "Mexicana"},
    {"id": 4, "nombre": "🍣 Sushi Roll", "precio": 15.99, "categoria": "Japonesa"},
    {"id": 5, "nombre": "🥗 Ensalada César", "precio": 7.99, "categoria": "Ensaladas"},
    {"id": 6, "nombre": "🍝 Pasta Alfredo", "precio": 11.99, "categoria": "Pastas"},
    {"id": 7, "nombre": "🐟 Ceviche", "precio": 13.99, "categoria": "Peruana"},
    {"id": 8, "nombre": "🥤 Bebida Gaseosa", "precio": 2.50, "categoria": "Bebidas"},
    {"id": 9, "nombre": "🍰 Pastel de Chocolate", "precio": 5.99, "categoria": "Postres"},
    {"id": 10, "nombre": "🍦 Helado de Vainilla", "precio": 3.99, "categoria": "Postres"},
]

# HTML del Dashboard
DASHBOARD_HTML = '''
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🍕 FoodDash - Pedidos de Comida</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        
        .header {
            background: white;
            padding: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .header-content {
            max-width: 1200px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .logo h1 {
            color: #667eea;
            font-size: 24px;
        }
        
        .logo p {
            color: #666;
            font-size: 12px;
        }
        
        .cart-icon {
            position: relative;
            cursor: pointer;
            background: #667eea;
            padding: 10px 20px;
            border-radius: 25px;
            color: white;
            transition: transform 0.3s;
        }
        
        .cart-icon:hover {
            transform: scale(1.05);
            background: #5a67d8;
        }
        
        .cart-count {
            background: #ff6b6b;
            border-radius: 50%;
            padding: 2px 8px;
            font-size: 12px;
            margin-left: 8px;
        }
        
        .container {
            max-width: 1200px;
            margin: 40px auto;
            padding: 0 20px;
        }
        
        .categories {
            display: flex;
            gap: 10px;
            margin-bottom: 30px;
            flex-wrap: wrap;
        }
        
        .category-btn {
            padding: 10px 20px;
            border: none;
            background: white;
            border-radius: 25px;
            cursor: pointer;
            transition: all 0.3s;
            font-size: 14px;
        }
        
        .category-btn.active {
            background: #667eea;
            color: white;
        }
        
        .menu-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 20px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }
        
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        }
        
        .card h3 {
            color: #333;
            margin-bottom: 10px;
            font-size: 18px;
        }
        
        .card .categoria {
            color: #667eea;
            font-size: 12px;
            margin-bottom: 10px;
        }
        
        .card .precio {
            font-size: 24px;
            color: #48bb78;
            font-weight: bold;
            margin: 10px 0;
        }
        
        .btn-add {
            background: #667eea;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 25px;
            cursor: pointer;
            width: 100%;
            font-size: 14px;
            transition: background 0.3s;
        }
        
        .btn-add:hover {
            background: #5a67d8;
        }
        
        /* Modal del carrito */
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.5);
            z-index: 1000;
            justify-content: center;
            align-items: center;
        }
        
        .modal-content {
            background: white;
            border-radius: 20px;
            padding: 30px;
            max-width: 500px;
            width: 90%;
            max-height: 80vh;
            overflow-y: auto;
        }
        
        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        
        .close {
            font-size: 28px;
            cursor: pointer;
            color: #999;
        }
        
        .cart-item {
            display: flex;
            justify-content: space-between;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        
        .cart-total {
            font-size: 20px;
            font-weight: bold;
            margin: 20px 0;
            text-align: right;
        }
        
        .btn-checkout {
            background: #48bb78;
            color: white;
            border: none;
            padding: 15px;
            border-radius: 10px;
            width: 100%;
            font-size: 16px;
            cursor: pointer;
        }
        
        .btn-checkout:hover {
            background: #38a169;
        }
        
        .pedidos-section {
            margin-top: 40px;
            background: white;
            border-radius: 15px;
            padding: 20px;
        }
        
        .pedidos-section h2 {
            margin-bottom: 20px;
            color: #333;
        }
        
        .pedido-card {
            background: #f7f7f7;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 10px;
        }
        
        .footer {
            text-align: center;
            padding: 20px;
            background: white;
            margin-top: 40px;
        }
        
        @media (max-width: 768px) {
            .menu-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="header-content">
            <div class="logo">
                <h1>🍕 FoodDash</h1>
                <p>Tu comida favorita a un clic</p>
            </div>
            <div class="cart-icon" onclick="abrirCarrito()">
                🛒 Carrito
                <span class="cart-count" id="cartCount">0</span>
            </div>
        </div>
    </div>
    
    <div class="container">
        <div class="categories" id="categories">
            <button class="category-btn active" data-categoria="Todas">Todas</button>
        </div>
        
        <div class="menu-grid" id="menuGrid"></div>
        
        <div class="pedidos-section">
            <h2>📋 Pedidos Realizados</h2>
            <div id="pedidosList"></div>
        </div>
    </div>
    
    <!-- Modal Carrito -->
    <div id="cartModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>🛒 Tu Carrito</h2>
                <span class="close" onclick="cerrarCarrito()">&times;</span>
            </div>
            <div id="cartItems"></div>
            <div class="cart-total" id="cartTotal">Total: $0.00</div>
            <button class="btn-checkout" onclick="realizarPedido()">✅ Realizar Pedido</button>
        </div>
    </div>
    
    <div class="footer">
        <p>🍕 FoodDash - Tu comida rápida favorita © 2026</p>
    </div>
    
    <script>
        let menu = {{ menu|tojson }};
        let carrito = [];
        let pedidos = [];
        
        function cargarCategorias() {
            const categorias = ['Todas', ...new Set(menu.map(item => item.categoria))];
            const container = document.getElementById('categories');
            container.innerHTML = categorias.map(cat => 
                `<button class="category-btn" data-categoria="${cat}">${cat}</button>`
            ).join('');
            
            document.querySelectorAll('.category-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    document.querySelectorAll('.category-btn').forEach(b => b.classList.remove('active'));
                    btn.classList.add('active');
                    filtrarMenu(btn.dataset.categoria);
                });
            });
        }
        
        function filtrarMenu(categoria) {
            let filtered = menu;
            if (categoria !== 'Todas') {
                filtered = menu.filter(item => item.categoria === categoria);
            }
            mostrarMenu(filtered);
        }
        
        function mostrarMenu(items) {
            const grid = document.getElementById('menuGrid');
            grid.innerHTML = items.map(item => `
                <div class="card">
                    <h3>${item.nombre}</h3>
                    <div class="categoria">${item.categoria}</div>
                    <div class="precio">$${item.precio.toFixed(2)}</div>
                    <button class="btn-add" onclick="agregarAlCarrito(${item.id})">Agregar al Carrito</button>
                </div>
            `).join('');
        }
        
        function agregarAlCarrito(id) {
            const producto = menu.find(p => p.id === id);
            const existente = carrito.find(p => p.id === id);
            
            if (existente) {
                existente.cantidad++;
            } else {
                carrito.push({...producto, cantidad: 1});
            }
            
            actualizarContadorCarrito();
            mostrarNotificacion(`✅ ${producto.nombre} agregado al carrito`);
        }
        
        function actualizarContadorCarrito() {
            const total = carrito.reduce((sum, item) => sum + item.cantidad, 0);
            document.getElementById('cartCount').textContent = total;
        }
        
        function abrirCarrito() {
            mostrarCarrito();
            document.getElementById('cartModal').style.display = 'flex';
        }
        
        function cerrarCarrito() {
            document.getElementById('cartModal').style.display = 'none';
        }
        
        function mostrarCarrito() {
            const container = document.getElementById('cartItems');
            if (carrito.length === 0) {
                container.innerHTML = '<p>El carrito está vacío</p>';
                document.getElementById('cartTotal').textContent = 'Total: $0.00';
                return;
            }
            
            container.innerHTML = carrito.map(item => `
                <div class="cart-item">
                    <span>${item.nombre} x${item.cantidad}</span>
                    <span>$${(item.precio * item.cantidad).toFixed(2)}</span>
                </div>
            `).join('');
            
            const total = carrito.reduce((sum, item) => sum + (item.precio * item.cantidad), 0);
            document.getElementById('cartTotal').textContent = `Total: $${total.toFixed(2)}`;
        }
        
        function realizarPedido() {
            if (carrito.length === 0) {
                alert('El carrito está vacío');
                return;
            }
            
            const total = carrito.reduce((sum, item) => sum + (item.precio * item.cantidad), 0);
            const pedido = {
                id: Date.now(),
                fecha: new Date().toLocaleString(),
                items: [...carrito],
                total: total,
                estado: '🟡 En preparación'
            };
            
            pedidos.unshift(pedido);
            actualizarListaPedidos();
            
            carrito = [];
            actualizarContadorCarrito();
            cerrarCarrito();
            
            mostrarNotificacion('🎉 Pedido realizado con éxito!');
        }
        
        function actualizarListaPedidos() {
            const container = document.getElementById('pedidosList');
            if (pedidos.length === 0) {
                container.innerHTML = '<p>No hay pedidos aún</p>';
                return;
            }
            
            container.innerHTML = pedidos.map(pedido => `
                <div class="pedido-card">
                    <strong>Pedido #${pedido.id}</strong><br>
                    📅 ${pedido.fecha}<br>
                    📦 Items: ${pedido.items.length}<br>
                    💰 Total: $${pedido.total.toFixed(2)}<br>
                    ${pedido.estado}
                </div>
            `).join('');
        }
        
        function mostrarNotificacion(mensaje) {
            const notif = document.createElement('div');
            notif.textContent = mensaje;
            notif.style.cssText = `
                position: fixed;
                bottom: 20px;
                right: 20px;
                background: #48bb78;
                color: white;
                padding: 12px 24px;
                border-radius: 8px;
                z-index: 2000;
                animation: fadeOut 3s forwards;
            `;
            document.body.appendChild(notif);
            setTimeout(() => notif.remove(), 3000);
        }
        
        // Inicializar
        cargarCategorias();
        mostrarMenu(menu);
        actualizarListaPedidos();
        
        // Cerrar modal al hacer clic fuera
        window.onclick = function(event) {
            const modal = document.getElementById('cartModal');
            if (event.target === modal) {
                cerrarCarrito();
            }
        }
    </script>
    
    <style>
        @keyframes fadeOut {
            0% { opacity: 1; }
            70% { opacity: 1; }
            100% { opacity: 0; visibility: hidden; }
        }
    </style>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(DASHBOARD_HTML, menu=MENU)

@app.route('/pedido', methods=['POST'])
def hacer_pedido():
    data = request.json
    pedido = {
        "id": datetime.now().timestamp(),
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "items": data.get('items', []),
        "total": data.get('total', 0)
    }
    return jsonify({"status": "success", "pedido": pedido})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)