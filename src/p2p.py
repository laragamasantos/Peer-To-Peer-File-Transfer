import socket
import threading
import time
import json
import base64
import os

BLOCK_SIZE = 100000  # bytes

class Peer:
    def __init__(self, host, port, id, datas=None):
        self.host = host
        self.port = port
        self.id = id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = []

        if datas is None:
            self.datas = {}
        else:
            self.datas = datas

        self.received_blocks = {}  # blocos recebidos por arquivo
        self.total_blocks = {}     # total de blocos esperados

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

    def split_file(self, file_path):
        file_name = os.path.basename(file_path)
        blocks = []
        with open(file_path, "rb") as f:
            i = 0
            while True:
                block = f.read(BLOCK_SIZE)
                if not block:
                    break
                block_name = f"{file_name}_part{i}"
                block_path = f"files/{self.id}/{block_name}"
                os.makedirs(os.path.dirname(block_path), exist_ok=True)
                with open(block_path, "wb") as bf:
                    bf.write(block)
                blocks.append(block_path)
                i += 1
        print(f"{self.id} - Split '{file_name}' into {len(blocks)} blocks.")
        return file_name, blocks

    def send_data(self, file_path, requester_id, connection):
        try:
            file_name, blocks = self.split_file(file_path)
            total_blocks = len(blocks)

            for i, block_path in enumerate(blocks):
                block_name = os.path.basename(block_path)
                with open(block_path, "rb") as bf:
                    block_bytes = bf.read()
                    encoded = base64.b64encode(block_bytes).decode('utf-8')

                message = {
                    "type": "send_block",
                    "file_name": file_name,
                    "block_name": block_name,
                    "block_index": i,
                    "total_blocks": total_blocks,
                    "block_content": encoded,
                    "requester_id": requester_id,
                    "sender_id": self.id
                }

                connection.sendall((json.dumps(message) + "\n").encode())
                print(f"{self.id} - Sent block {i+1}/{total_blocks} ('{block_name}') to {requester_id}")
            
            for block_path in blocks:
                try:
                    os.remove(block_path)
                except Exception as e:
                    print(f"{self.id} - Failed to remove block '{block_path}': {e}")

        except Exception as e:
            print(f"{self.id} - Error sending file blocks: {e}")
            if connection in self.connections:
                self.connections.remove(connection)

    def request_data(self, file_name):
        print(f"{self.id} - Requesting file '{file_name}' from peers.")
        request = {
            "type": "request_file",
            "file_name": file_name,
            "requester_id": self.id
        }

        request = json.dumps(request)
        for connection in self.connections:
            connection.sendall((request + "\n").encode())

    def reconstruct_file(self, file_name):
        parts = sorted(
            [f for f in os.listdir(f"files/{self.id}") if f.startswith(file_name + "_part")],
            key=lambda x: int(x.split("_part")[-1])
        )

        output_path = f"files/{self.id}/{file_name}"
        with open(output_path, "wb") as outfile:
            for part in parts:
                with open(f"files/{self.id}/{part}", "rb") as pf:
                    outfile.write(pf.read())

        print(f"{self.id} - '{output_path}' from {len(parts)} blocks.")
        
        for part in parts:
            try:
                os.remove(f"files/{self.id}/{part}")
            except Exception as e:
                print(f"{self.id} - Failed to remove block '{part}': {e}")

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

                    if data["type"] == "request_file":
                        file_name = data["file_name"]
                        file_path = f"files/{self.id}/{file_name}"
                        if os.path.exists(file_path):
                            self.send_data(file_path, data["requester_id"], connection)
                        else:
                            print(f"{self.id} - File '{file_name}' not found to send.")

                    elif data["type"] == "send_block" and data["requester_id"] == self.id:
                        file_name = data["file_name"]
                        block_name = data["block_name"]
                        total_blocks = data["total_blocks"]
                        block_bytes = base64.b64decode(data["block_content"])

                        os.makedirs(f"files/{self.id}", exist_ok=True)
                        save_path = f"files/{self.id}/{block_name}"

                        with open(save_path, "wb") as f:
                            f.write(block_bytes)

                        self.received_blocks.setdefault(file_name, set()).add(block_name)
                        self.total_blocks[file_name] = total_blocks

                        print(f"{self.id} - Received block '{block_name}' ({len(self.received_blocks[file_name])}/{total_blocks})")

                        if len(self.received_blocks[file_name]) == total_blocks:
                            self.reconstruct_file(file_name)

            except (socket.error, json.JSONDecodeError) as e:
                break

        print(f"Connection from {address} closed.")
        if connection in self.connections:
            self.connections.remove(connection)
        connection.close()

    def start(self):
        listen_thread = threading.Thread(target=self.listen, daemon=True)
        listen_thread.start()

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

    for peer in peers.values():
        for otherPeer in peers.values():
            if peer.port < otherPeer.port:
                peer.connect("127.0.0.1", otherPeer.port)
                time.sleep(0.1)
                
    while True:
        peer_id = input("Digite o id do Peer que vai fazer a requisição: ")
        file_name = input("Qual o nome do arquivo: ")
        peers[peer_id].request_data(file_name)
