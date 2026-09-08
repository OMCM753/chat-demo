"""
Nodo de chat grupal mesh, con descubrimiento automático (mDNS) + Peer
Exchange (PEX) + bootstrap opcional por directorio o por peer.

Cada usuario corre una instancia de este script. El nodo descubre peers
de TRES formas, que se pueden combinar:

1. mDNS (automático, cero configuración): si hay otros nodos en la MISMA
   red local (WiFi/LAN), se descubren solos, sin URLs ni servidores.
   Activado por defecto; desactívalo con --no-lan.

2. --bootstrap-peer: le pregunta directamente a UN nodo que ya está en
   la red (útil para unirse desde otra red, por internet).

3. --directory: le pregunta a un directorio central quién está activo
   (útil solo la primera vez que arranca la red desde cero).

Una vez dentro por cualquiera de las tres vías, PEER EXCHANGE (PEX) se
encarga de seguir descubriendo peers nuevos preguntándole a los peers
actuales quiénes más conocen ellos -- así ninguno de los tres mecanismos
de entrada sigue siendo necesario después del primer contacto.

Cuando el usuario envía un mensaje, se difunde (gossip/flooding) a todos
los peers conocidos, y sirve una interfaz web de chat en "/".

Uso típico en la misma red WiFi (solo mDNS, sin flags extra):
    python mesh_chat_node.py --port 5000 --id ana --public-url http://localhost:5000

Uso entre redes distintas, con un peer conocido:
    python mesh_chat_node.py --port 5000 --id beto \
        --bootstrap-peer https://xxxx.lhr.life \
        --public-url https://yyyy.lhr.life
"""

import argparse
import random
import socket
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template_string, request
import requests

try:
    from zeroconf import (
        ServiceBrowser,
        ServiceInfo,
        ServiceListener,
        Zeroconf,
    )

    ZEROCONF_AVAILABLE = True
except ImportError:
    ZEROCONF_AVAILABLE = False

app = Flask(__name__)

MDNS_SERVICE_TYPE = "_meshchat._tcp.local."

# --- Estado global del nodo ---
NODE_ID = None
PUBLIC_URL = None
DIRECTORY_URL = None
BOOTSTRAP_PEER = None

PEERS = {}  # {node_id: url}
PEER_FAILURES = {}  # {node_id: intentos_fallidos}
MESSAGES = []  # [{id, from, content, time}]
SEEN_IDS = set()  # IDs de mensajes procesados

# Locks para asegurar Thread-Safety
PEERS_LOCK = threading.Lock()
DATA_LOCK = threading.Lock()

HEARTBEAT_INTERVAL = 20  # segundos entre anuncios al directorio
PEX_INTERVAL = 15  # segundos entre rondas de peer exchange
PEX_SAMPLE_SIZE = 3  # muestra de peers a consultar en PEX
MAX_FAILURES = 3  # fallos para descartar a un peer


CHAT_HTML = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>Mesh Chat - {{ node_id }}</title>
  <style>
    body { font-family: sans-serif; max-width: 600px; margin: 40px auto; }
    #messages { border: 1px solid #ccc; height: 400px; overflow-y: auto; padding: 10px; }
    .msg { margin-bottom: 8px; }
    .from { font-weight: bold; color: #2563eb; }
    .time { color: #888; font-size: 0.8em; margin-left: 6px; }
    #peers { color: #555; font-size: 0.9em; margin-bottom: 10px; }
    input[type=text] { width: 80%; padding: 8px; }
    button { padding: 8px 16px; }
  </style>
</head>
<body>
  <h2>Mesh Chat -- soy "{{ node_id }}"</h2>
  <div id="peers">Peers conocidos: cargando...</div>
  <div id="messages"></div>
  <br>
  <input type="text" id="content" placeholder="Escribe un mensaje..." onkeydown="if(event.key==='Enter') sendMsg()">
  <button onclick="sendMsg()">Enviar</button>

  <script>
    async function refresh() {
      const res = await fetch('/messages');
      const data = await res.json();
      const box = document.getElementById('messages');
      box.innerHTML = data.messages.map(m => 
        `<div class="msg"><span class="from">${m.from}:</span> ${m.content} <span class="time">${m.time}</span></div>`
      ).join('');
      box.scrollTop = box.scrollHeight;

      const peersRes = await fetch('/peers');
      const peersData = await peersRes.json();
      const names = Object.keys(peersData.peers).filter(n => n !== "{{ node_id }}");
      document.getElementById('peers').innerText = 
        'Peers conocidos: ' + (names.join(', ') || '(ninguno todavía)');
    }

    async function sendMsg() {
      const input = document.getElementById('content');
      const content = input.value.trim();
      if (!content) return;
      await fetch('/send', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({content})
      });
      input.value = '';
      refresh();
    }

    setInterval(refresh, 2000);
    refresh();
  </script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def chat_page():
    return render_template_string(CHAT_HTML, node_id=NODE_ID)


@app.route("/health", methods=["GET"])
def health():
    with PEERS_LOCK:
        peer_count = len(PEERS)
    return jsonify({"status": "ok", "node_id": NODE_ID, "peers": peer_count})


@app.route("/peers", methods=["GET"])
def get_peers():
    with PEERS_LOCK:
        peers_with_self = dict(PEERS)
    peers_with_self[NODE_ID] = PUBLIC_URL
    return jsonify({"peers": peers_with_self})


@app.route("/messages", methods=["GET"])
def get_messages():
    with DATA_LOCK:
        msgs = list(MESSAGES)
    return jsonify({"messages": msgs})


def _merge_peer(node_id, url):
    if node_id == NODE_ID or not url:
        return
    with PEERS_LOCK:
        if node_id not in PEERS:
            PEERS[node_id] = url
            PEER_FAILURES[node_id] = 0
            print(f"[{NODE_ID}] Nuevo peer descubierto: {node_id} ({url})")


def _mark_peer_result(node_id, success):
    with PEERS_LOCK:
        if node_id not in PEERS:
            return
        if success:
            PEER_FAILURES[node_id] = 0
        else:
            PEER_FAILURES[node_id] = PEER_FAILURES.get(node_id, 0) + 1
            if PEER_FAILURES[node_id] >= MAX_FAILURES:
                print(f"[{NODE_ID}] Descartando peer inactivo: {node_id}")
                del PEERS[node_id]
                del PEER_FAILURES[node_id]


def _relay_to_peers(msg):
    """Ejecuta el envío HTTP a peers en un hilo en segundo plano (asíncrono)."""
    with PEERS_LOCK:
        targets = list(PEERS.items())

    for peer_id, peer_url in targets:
        try:
            requests.post(f"{peer_url}/receive", json=msg, timeout=3)
            _mark_peer_result(peer_id, success=True)
        except requests.exceptions.RequestException:
            _mark_peer_result(peer_id, success=False)


def _store_and_relay(msg):
    """Guarda localmente el mensaje y dispara la retransmisión Gossip."""
    with DATA_LOCK:
        if msg["id"] in SEEN_IDS:
            return False
        SEEN_IDS.add(msg["id"])
        MESSAGES.append(msg)

    # Disparar Gossip en un hilo secundario para no bloquear la petición HTTP actual
    threading.Thread(
        target=_relay_to_peers, args=(msg,), daemon=True
    ).start()
    return True


@app.route("/receive", methods=["POST"])
def receive():
    data = request.get_json(silent=True) or {}
    msg = {
        "id": data.get("id", str(uuid.uuid4())),
        "from": data.get("from", "desconocido"),
        "content": data.get("content", ""),
        "time": data.get(
            "time", datetime.now(timezone.utc).strftime("%H:%M:%S")
        ),
    }
    _store_and_relay(msg)
    return jsonify({"ok": True})


@app.route("/send", methods=["POST"])
def send():
    data = request.get_json(silent=True) or {}
    msg = {
        "id": str(uuid.uuid4()),
        "from": NODE_ID,
        "content": data.get("content", ""),
        "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
    }
    _store_and_relay(msg)
    return jsonify({"ok": True, "message": msg})


def bootstrap_from_directory():
    try:
        requests.post(
            f"{DIRECTORY_URL}/announce",
            json={"node_id": NODE_ID, "url": PUBLIC_URL},
            timeout=5,
        )
        resp = requests.get(
            f"{DIRECTORY_URL}/nodes",
            params={"exclude": NODE_ID},
            timeout=5,
        )
        if resp.ok:
            for nid, url in resp.json().get("nodes", {}).items():
                _merge_peer(nid, url)
    except requests.exceptions.RequestException as e:
        print(f"[{NODE_ID}] Error contactando directorio: {e}")


def bootstrap_from_peer():
    try:
        resp = requests.get(f"{BOOTSTRAP_PEER}/peers", timeout=5)
        if resp.ok:
            for nid, url in resp.json().get("peers", {}).items():
                _merge_peer(nid, url)
            print(
                f"[{NODE_ID}] Bootstrap via peer exitoso ({len(PEERS)} peers conocidos)"
            )
    except requests.exceptions.RequestException as e:
        print(f"[{NODE_ID}] Error en bootstrap via peer: {e}")


def directory_heartbeat_loop():
    while True:
        bootstrap_from_directory()
        time.sleep(HEARTBEAT_INTERVAL)


def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


class _MeshDiscoveryListener(
    ServiceListener if ZEROCONF_AVAILABLE else object
):

    def add_service(self, zc, service_type, name):
        self._handle(zc, service_type, name)

    def update_service(self, zc, service_type, name):
        self._handle(zc, service_type, name)

    def remove_service(self, zc, service_type, name):
        pass

    def _handle(self, zc, service_type, name):
        info = zc.get_service_info(service_type, name)
        if not info:
            return
        node_id = info.properties.get(b"node_id", b"").decode("utf-8")
        if not node_id or node_id == NODE_ID:
            return
        ip = socket.inet_ntoa(info.addresses[0])
        url = f"http://{ip}:{info.port}"
        _merge_peer(node_id, url)


def start_mdns_discovery(port):
    if not ZEROCONF_AVAILABLE:
        print(f"[{NODE_ID}] zeroconf no instalado -- mDNS deshabilitado.")
        return None

    local_ip = get_local_ip()
    info = ServiceInfo(
        MDNS_SERVICE_TYPE,
        f"{NODE_ID}.{MDNS_SERVICE_TYPE}",
        addresses=[socket.inet_aton(local_ip)],
        port=port,
        properties={"node_id": NODE_ID},
    )

    zc = Zeroconf()
    zc.register_service(info)
    ServiceBrowser(zc, MDNS_SERVICE_TYPE, _MeshDiscoveryListener())
    print(
        f"[{NODE_ID}] mDNS activo -- anunciándose en la LAN ({local_ip}:{port})"
    )
    return zc


def pex_loop():
    while True:
        time.sleep(PEX_INTERVAL)

        with PEERS_LOCK:
            sample = list(PEERS.items())
        random.shuffle(sample)
        sample = sample[:PEX_SAMPLE_SIZE]

        for peer_id, peer_url in sample:
            try:
                resp = requests.get(f"{peer_url}/peers", timeout=3)
                if resp.ok:
                    for nid, url in resp.json().get("peers", {}).items():
                        _merge_peer(nid, url)
                    _mark_peer_result(peer_id, success=True)
            except requests.exceptions.RequestException:
                _mark_peer_result(peer_id, success=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Nodo de chat grupal mesh con PEX"
    )
    parser.add_argument("--port", type=int, default=5000, help="Puerto local")
    parser.add_argument(
        "--id", type=str, required=True, help="Tu nombre/id en el chat"
    )
    parser.add_argument(
        "--public-url",
        type=str,
        default=None,
        help="Tu URL pública (ej. ngrok). Si no se indica, usa la IP local.",
    )
    parser.add_argument(
        "--directory",
        type=str,
        default=None,
        help="URL del directorio central",
    )
    parser.add_argument(
        "--bootstrap-peer",
        type=str,
        default=None,
        help="URL de un peer ya activo",
    )
    parser.add_argument(
        "--no-lan",
        action="store_true",
        help="Desactiva mDNS en la red local",
    )
    args = parser.parse_args()

    NODE_ID = args.id
    PUBLIC_URL = (
        args.public_url.rstrip("/")
        if args.public_url
        else f"http://{get_local_ip()}:{args.port}"
    )
    DIRECTORY_URL = args.directory.rstrip("/") if args.directory else None
    BOOTSTRAP_PEER = (
        args.bootstrap_peer.rstrip("/") if args.bootstrap_peer else None
    )

    if BOOTSTRAP_PEER:
        bootstrap_from_peer()

    if DIRECTORY_URL:
        threading.Thread(target=directory_heartbeat_loop, daemon=True).start()

    zc_instance = None
    if not args.no_lan:
        zc_instance = start_mdns_discovery(args.port)

    threading.Thread(target=pex_loop, daemon=True).start()

    print(f"--- Nodo '{NODE_ID}' en ejecución ---")
    print(f"Acceso Web: http://localhost:{args.port}")

    try:
        app.run(host="0.0.0.0", port=args.port, debug=False)
    finally:
        if zc_instance:
            zc_instance.close()
