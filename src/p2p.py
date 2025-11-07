import socket
import threading
import time
import json
import base64
import os

class Peer:
    def __init__(self, host, port, id):
        self.host = host
        self.port = port
        self.id = id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = []

        # Diretório base de arquivos deste peer
        self.base_dir = os.path.join(os.path.dirname(__file__), "..", "files", self.id)
        os.makedirs(self.base_dir, exist_ok=True)

    def connect(self, peer_host, peer_port):
        connection = socket.create_connection((peer_host, peer_port))
        self.connections.append(connection)
        threading.Thread(target=self.handle_client, args=(connection, (peer_host, peer_port)), daemon=True).start()

    def listen(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        while True:
            connection, address = self.socket.accept()
            self.connections.append(connection)
            threading.Thread(target=self.handle_client, args=(connection, address), daemon=True).start()

    # 🔹 Envia arquivo (busca dentro da pasta /files/<peer_id>)
    def send_file(self, file_name, requester_id, connection):
        try:
            file_path = os.path.join(self.base_dir, file_name)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"{file_path} not found")

            with open(file_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            message = {
                "type": "send_file",
                "file_name": file_name,
                "file_content": encoded,
                "requester_id": requester_id,
                "sender_id": self.id
            }

            connection.sendall((json.dumps(message) + "\n").encode())
            print(f"{self.id} - Sent file '{file_name}' to {requester_id}")

        except Exception as e:
            print(f"{self.id} - Failed to send file '{file_name}': {e}")
            if connection in self.connections:
                self.connections.remove(connection)

    # 🔹 Solicita arquivo de outros peers
    def request_file(self, file_name):
        print(f"{self.id} - Requesting file '{file_name}' from peers.")
        message = {
            "type": "request_file",
            "file_name": file_name,
            "requester_id": self.id
        }
        for connection in self.connections:
            connection.sendall((json.dumps(message) + "\n").encode())

    # 🔹 Trata mensagens recebidas
    def handle_client(self, connection, address):
        buffer = ""
        while True:
            try:
                chunk = connection.recv(1024)
                if not chunk:
                    break

                buffer += chunk.decode()

                while "\n" in buffer:
                    msg, buffer = buffer.split("\n", 1)
                    if not msg.strip():
                        continue

                    data = json.loads(msg)

                    # --- Pedido de arquivo ---
                    if data["type"] == "request_file":
                        file_name = data["file_name"]
                        self.send_file(file_name, data["requester_id"], connection)

                    # --- Recebimento de arquivo ---
                    elif data["type"] == "send_file" and data["requester_id"] == self.id:
                        file_name = data["file_name"]
                        file_bytes = base64.b64decode(data["file_content"])
                        try:
                            save_path = os.path.join(self.base_dir, file_name)
                            with open(save_path, "wb") as f:
                                f.write(file_bytes)
                            print(f"{self.id} - Received '{file_name}' from {data['sender_id']} (saved to {save_path})")
                        except Exception as e:
                            print(f"{self.id} - Error saving file '{file_name}': {e}")

            except (socket.error, json.JSONDecodeError):
                break

        print(f"Connection from {address} closed.")
        if connection in self.connections:
            self.connections.remove(connection)
        connection.close()

    def start(self):
        threading.Thread(target=self.listen, daemon=True).start()


# --- Execução principal ---
if __name__ == "__main__":
    peers = {
        "A": Peer("0.0.0.0", 8001, "A"),
        "B": Peer("0.0.0.0", 8002, "B"),
        "C": Peer("0.0.0.0", 8003, "C"),
        "D": Peer("0.0.0.0", 8004, "D"),
    }

    for peer in peers.values():
        peer.start()

    time.sleep(2)

    for p in peers.values():
        for other in peers.values():
            if p.port < other.port:
                p.connect("127.0.0.1", other.port)
                time.sleep(0.1)

    print("Rede inicializada.\n")

    while True:
        peer_id = input("Peer que vai solicitar o arquivo (A/B/C/D ou 0 para sair): ")
        if peer_id == "0":
            break
        file_name = input("Nome do arquivo (ex: teste.txt): ")
        peers[peer_id].request_file(file_name)
        print()
