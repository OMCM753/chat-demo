"""
Directorio central para la red mesh de chat.

Este servicio NO participa del chat ni almacena mensajes. Su único trabajo
es llevar la lista de nodos actualmente activos, para que cada nodo pueda
descubrir a los demás sin necesidad de registro manual.

Debe correr en una URL ESTABLE conocida por todos los nodos de antemano
(por ejemplo, desplegado en Render, Railway, PythonAnywhere, un VPS, etc.).
Si tú mismo lo corres localmente, expónlo con localhost.run/ngrok UNA VEZ
y comparte esa URL con quienes se vayan a unir al chat -- esa es la única
dirección que todo el mundo necesita saber de antemano.

Uso:
    python directory.py --port 6000
"""

import argparse
import time
from datetime import datetime

from flask import Flask, jsonify, request

app = Flask(__name__)

# {node_id: {"url": str, "last_seen": float}}
NODES = {}

# Un nodo se considera "muerto" si no se anuncia en este tiempo (segundos)
NODE_TIMEOUT = 60


def cleanup_stale_nodes():
    """Elimina nodos que no se han anunciado recientemente."""
    now = time.time()
    stale = [nid for nid, info in NODES.items() if now - info["last_seen"] > NODE_TIMEOUT]
    for nid in stale:
        del NODES[nid]


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@app.route("/announce", methods=["POST"])
def announce():
    """
    Un nodo llama esto periódicamente (heartbeat) para anunciarse.

    Body JSON esperado:
    {
        "node_id": "usuario123",
        "url": "https://xxxx.lhr.life"
    }
    """
    data = request.get_json(force=True) or {}
    node_id = data.get("node_id")
    url = data.get("url")

    if not node_id or not url:
        return jsonify({"error": "Se requieren 'node_id' y 'url'"}), 400

    NODES[node_id] = {"url": url.rstrip("/"), "last_seen": time.time()}
    cleanup_stale_nodes()

    return jsonify({"message": "Anunciado correctamente", "active_nodes": len(NODES)})


@app.route("/nodes", methods=["GET"])
def list_nodes():
    """Devuelve la lista de nodos activos (excluyendo al que pregunta, si se indica)."""
    cleanup_stale_nodes()
    exclude = request.args.get("exclude")
    nodes = {nid: info["url"] for nid, info in NODES.items() if nid != exclude}
    return jsonify({"nodes": nodes, "count": len(nodes)})


@app.route("/leave", methods=["POST"])
def leave():
    """Un nodo puede avisar explícitamente que se desconecta (opcional)."""
    data = request.get_json(force=True) or {}
    node_id = data.get("node_id")
    if node_id in NODES:
        del NODES[node_id]
    return jsonify({"message": "Nodo removido"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Directorio central de la red mesh")
    parser.add_argument("--port", type=int, default=6000)
    args = parser.parse_args()
    print(f"Directorio corriendo en puerto {args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=True)
