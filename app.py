"""
Serviço de Mensagens Distribuídas com Consistência Eventual e Autenticação Básica
Arquivo: app.py

Resumo:
- Cada instância deste script é um "nó".
- Comunicação entre nós via HTTP (requests). HTTP roda sobre TCP, atendendo ao requisito de sockets TCP.
- Cada nó mantém uma cópia local do mural (messages).
- Postagens são replicadas assincronamente para peers.
- Suporta autenticação simples (login por usuário/senha -> token).
- Permite simular queda temporária (ignorar replicações) e reconciliar mensagens ao reconectar.
- Persistência simples: salva mensagens em arquivo JSON para sobreviver reinícios (opcional).
"""

import argparse
import threading
import requests
import time
import json
import os
import uuid
from datetime import datetime
from flask import Flask, request, jsonify, abort

app = Flask(__name__)

# ---------- Configuração e Estado do Nó ----------
lock = threading.Lock()  # trava para acesso concorrente ao mural e outros estados

class NodeState:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        # peers: lista de "http://host:port"
        self.peers = peers

        # messages: lista de dicts: {id, node_id, counter, timestamp_iso, user, text}
        self.messages = []
        # message_ids para deduplicação rápida
        self.message_ids = set()
        # counter local para criar IDs únicos
        self.local_counter = 0

        # Simple user store: username -> password (plain for demo).
        # Em produção: use hashing seguro (bcrypt) e store persistente.
        self.users = {
            "alice": "password1",
            "bob": "password2",
            "carol": "password3"
        }
        # sessions: token -> username
        self.sessions = {}

        # If accept_replication is False -> simulate node down for replication
        self.accept_replication = True

        # persistence filename
        self.persist_file = f"messages_{self.node_id}.json"
        # load on startup
        self._load_persisted()

    def _load_persisted(self):
        if os.path.exists(self.persist_file):
            try:
                with open(self.persist_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.messages = data.get("messages", [])
                    self.message_ids = set(m["id"] for m in self.messages)
                    self.local_counter = data.get("local_counter", self.local_counter)
                print(f"[{self.node_id}] Loaded {len(self.messages)} persisted messages.")
            except Exception as e:
                print(f"[{self.node_id}] Erro ao carregar persistência: {e}")

    def _persist(self):
        try:
            with open(self.persist_file, "w", encoding="utf-8") as f:
                json.dump({
                    "messages": self.messages,
                    "local_counter": self.local_counter
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[{self.node_id}] Erro ao persistir: {e}")

state = None  # será inicializado no main

# ---------- Utilitários ----------
def create_message(username, text):
    """Cria um novo objeto de mensagem com ID único usando node_id e contador."""
    with lock:
        state.local_counter += 1
        counter = state.local_counter
    msg_id = f"{state.node_id}-{counter}-{uuid.uuid4().hex[:6]}"
    timestamp = datetime.utcnow().isoformat() + "Z"
    return {
        "id": msg_id,
        "node_id": state.node_id,
        "counter": counter,
        "timestamp": timestamp,
        "user": username,
        "text": text
    }

def add_message_local(msg):
    """Adiciona mensagem localmente se ainda não existir (id unique)."""
    with lock:
        if msg["id"] in state.message_ids:
            return False
        state.messages.append(msg)
        state.message_ids.add(msg["id"])
        # opcional: ordenar por timestamp para leitura cronológica
        state.messages.sort(key=lambda m: m["timestamp"])
        # persistir após alteração
        state._persist()
        return True

def replicate_to_peer(peer_url, msg):
    """Envia uma replicação para um peer (POST /replicate)."""
    try:
        url = f"{peer_url.rstrip('/')}/replicate"
        # Timeout curto para não bloquear demais
        resp = requests.post(url, json={"message": msg, "from": state.node_id}, timeout=3)
        if resp.status_code == 200:
            return True
        else:
            print(f"[{state.node_id}] Replicação para {peer_url} retornou {resp.status_code}")
            return False
    except Exception as e:
        # print erro de rede (peer down ou não acessível)
        # para eventual consistência, não falhamos aqui; mensagens serão re-enviadas quando o nó for reconciliado.
        # Em produção, usar fila de retry com backoff.
        print(f"[{state.node_id}] Erro replicando para {peer_url}: {e}")
        return False

def async_replicate(msg):
    """Replica a mensagem assincronamente para todos os peers (não espera confirmação)."""
    # Para não bloquear a request do usuário, fazemos isso em threads separadas.
    def worker(peer):
        # Tenta replicar algumas vezes com backoff simples
        attempts = 3
        for i in range(attempts):
            ok = replicate_to_peer(peer, msg)
            if ok:
                return
            time.sleep(1 + i)  # backoff simples
    for peer in state.peers:
        t = threading.Thread(target=worker, args=(peer,), daemon=True)
        t.start()

def fetch_messages_from_peer(peer_url):
    """Pega todas as mensagens do peer (GET /messages) - usado para reconciliação."""
    try:
        url = f"{peer_url.rstrip('/')}/messages"
        resp = requests.get(url, timeout=4)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("messages", [])
        else:
            print(f"[{state.node_id}] fetch_messages_from_peer {peer_url} status {resp.status_code}")
            return []
    except Exception as e:
        print(f"[{state.node_id}] Erro ao buscar mensagens de {peer_url}: {e}")
        return []

def reconcile_with_peers():
    """
    Compara mensagens com peers e puxa as que faltam.
    Estratégia simples: para cada peer, pede todas as mensagens e adiciona as que faltam.
    (Ineficiente para grande escala, mas atende requisito de consistência eventual.)
    """
    print(f"[{state.node_id}] Iniciando reconciliação com peers...")
    total_added = 0
    for peer in state.peers:
        peer_messages = fetch_messages_from_peer(peer)
        # adiciona sem duplicar
        for msg in peer_messages:
            added = add_message_local(msg)
            if added:
                total_added += 1
    print(f"[{state.node_id}] Reconciliação completa. Mensagens adicionadas: {total_added}")
    return total_added

# ---------- Rotas HTTP (API do nó) ----------

@app.route("/login", methods=["POST"])
def route_login():
    """
    Login:
    - Request JSON: {"username": "...", "password": "..."}
    - Response: {"token": "..."} (use token no header Authorization: Bearer <token>)
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "JSON required"}), 400
    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        return jsonify({"error": "username and password required"}), 400
    # autenticação simples
    if state.users.get(username) == password:
        token = uuid.uuid4().hex
        with lock:
            state.sessions[token] = {"user": username, "created": time.time()}
        return jsonify({"token": token})
    else:
        return jsonify({"error": "invalid credentials"}), 401

def get_user_from_token():
    """Lê token do header Authorization: Bearer <token> e retorna username ou None."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    with lock:
        sess = state.sessions.get(token)
    if not sess:
        return None
    return sess["user"]

@app.route("/post", methods=["POST"])
def route_post():
    """
    Postar mensagem (somente usuários autenticados):
    - Header: Authorization: Bearer <token>
    - JSON: {"text": "mensagem"}
    """
    user = get_user_from_token()
    if not user:
        return jsonify({"error": "authentication required"}), 401
    data = request.get_json(force=True, silent=True)
    if not data or "text" not in data:
        return jsonify({"error": "text required"}), 400
    text = data["text"].strip()
    if not text:
        return jsonify({"error": "text cannot be empty"}), 400

    # cria mensagem local
    msg = create_message(user, text)
    added = add_message_local(msg)
    if not added:
        return jsonify({"error": "message already exists (duplicated id)"}), 409

    # replicação assíncrona para peers (não espera confirmação)
    threading.Thread(target=async_replicate, args=(msg,), daemon=True).start()
    return jsonify({"status": "ok", "message": msg}), 201

@app.route("/messages", methods=["GET"])
def route_messages():
    """
    Ler mensagens públicas (não requer autenticação).
    Retorna todas as mensagens locais (cópia do mural do nó).
    """
    with lock:
        # devolve cópia para segurança
        return jsonify({"messages": list(state.messages)})

@app.route("/replicate", methods=["POST"])
def route_replicate():
    """
    Endpoint usado por peers para replicar uma mensagem.
    Corpo JSON: {"message": {...}, "from": "<node_id>"}
    """
    # Simular queda: se accept_replication estiver desligado, fingimos que o nó está offline
    if not state.accept_replication:
        # Respondemos 503 para indicar indisponibilidade (ou poderíamos simplesmente ignorar)
        return jsonify({"error": "node not accepting replication (simulated down)"}), 503

    data = request.get_json(force=True, silent=True)
    if not data or "message" not in data:
        return jsonify({"error": "message required"}), 400
    msg = data["message"]
    added = add_message_local(msg)
    if added:
        # opcional: log
        print(f"[{state.node_id}] Mensagem replicada recebida: {msg['id']} from {data.get('from')}")
    # Retorna 200 sempre que possível — o replicador faz retries se necessário.
    return jsonify({"status": "ok", "added": added})

@app.route("/simulate_fail", methods=["POST"])
def route_simulate_fail():
    """
    Simular queda temporária do nó para replicação.
    JSON: {"action": "down"} ou {"action": "up"}
    - "down": passa a rejeitar /replicate (simula queda)
    - "up": volta a aceitar e automaticamente reconcilia com peers
    """
    data = request.get_json(force=True, silent=True)
    if not data or "action" not in data:
        return jsonify({"error": "action required ('down' or 'up')"}), 400
    action = data["action"]
    if action == "down":
        state.accept_replication = False
        return jsonify({"status": "node replication disabled (simulated down)"}), 200
    elif action == "up":
        state.accept_replication = True
        # ao voltar, dispara reconciliação em background
        t = threading.Thread(target=reconcile_with_peers, daemon=True)
        t.start()
        return jsonify({"status": "node replication enabled; reconciling with peers started"}), 200
    else:
        return jsonify({"error": "unknown action"}), 400

@app.route("/reconcile", methods=["POST"])
def route_reconcile():
    """
    Forçar reconciliação manual com todos os peers (puxa mensagens que faltam).
    Útil depois de uma falha simulada ou reinício.
    """
    added = reconcile_with_peers()
    return jsonify({"status": "ok", "added": added})

@app.route("/peers", methods=["GET"])
def route_peers():
    """Retorna lista de peers conhecidos por este nó."""
    return jsonify({"peers": state.peers})

# ---------- Inicialização do Servidor ----------
def start_flask(host, port):
    # Para não mostrar logs do Werkzeug repetidos em threads, estamos chamando app.run diretamente.
    app.run(host=host, port=port, threaded=True)

def parse_args():
    parser = argparse.ArgumentParser(description="Nodo do mural distribuído (eventual consistency demo).")
    parser.add_argument("--node-id", required=True, help="ID único do nó (ex: node1)")
    parser.add_argument("--port", type=int, required=True, help="Porta HTTP do nó (ex: 5001)")
    parser.add_argument("--peers", default="", help="Lista de peers (URLs) separados por vírgula, ex: http://localhost:5002,http://localhost:5003")
    parser.add_argument("--host", default="0.0.0.0", help="Host para bind Flask (default 0.0.0.0)")
    return parser.parse_args()

def main():
    global state
    args = parse_args()
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    state = NodeState(node_id=args.node_id, port=args.port, peers=peers)

    print(f"[{state.node_id}] Iniciando nó na porta {args.port}. Peers: {state.peers}")
    # start Flask (bloqueia). Em cenários de produção, usar Gunicorn / uWSGI.
    start_flask(args.host, args.port)

if __name__ == "__main__":
    main()
