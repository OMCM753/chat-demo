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
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, request

app = Flask(__name__)

# Estado central: {node_id: {"url": str, "last_seen": float}}
NODES = {}
NODES_LOCK = threading.Lock()

# Tiempo (en segundos) tras el cual un nodo sin heartbeat se descarta
NODE_TIMEOUT = 60


def cleanup_stale_nodes():
    """Elimina del directorio los nodos que no hayan enviado heartbeat."""
    now = time.time()
    with NODES_LOCK:
        stale = [
            nid
            for nid, info in NODES.items()
            if now - info["last_seen"] > NODE_TIMEOUT
        ]
        for nid in stale:
            del NODES[nid]


@app.route("/health", methods=["GET"])
def health():
    return jsonify(
        {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}
    )


@app.route("/announce", methods=["POST"])
def announce():
    data = request.get_json(silent=True) or {}
    node_id = data.get("node_id")
    url = data.get("url")

    if not node_id or not url:
        return jsonify({"error": "Se requieren 'node_id' y 'url'"}), 400

    cleanup_stale_nodes()

    with NODES_LOCK:
        NODES[node_id] = {"url": url.rstrip("/"), "last_seen": time.time()}
        active_count = len(NODES)

    return jsonify(
        {"message": "Anunciado correctamente", "active_nodes": active_count}
    )


@app.route("/nodes", methods=["GET"])
def list_nodes():
    cleanup_stale_nodes()
    exclude = request.args.get("exclude")

    with NODES_LOCK:
        nodes = {
            nid: info["url"] for nid, info in NODES.items() if nid != exclude
        }

    return jsonify({"nodes": nodes, "count": len(nodes)})


@app.route("/leave", methods=["POST"])
def leave():
    data = request.get_json(silent=True) or {}
    node_id = data.get("node_id")

    with NODES_LOCK:
        if node_id in NODES:
            del NODES[node_id]

    return jsonify({"message": "Nodo removido"})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Directorio central de la red mesh"
    )
    parser.add_argument("--port", type=int, default=6000)
    args = parser.parse_args()

    print(f"Directorio corriendo en puerto {args.port}")
    app.run(host="0.0.0.0", port=args.port, debug=False)