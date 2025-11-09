import socket
import threading
import time
import json
import base64
import os
import hashlib
import shutil 

BLOCK_SIZE = 100000  # bytes

class Peer:
    def __init__(self, host, port, id, datas=None):
        self.host = host
        self.port = port
        self.id = id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = []
        self.received_meta = {}

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
    
    def compute_file_meta(self, file_path):
        sha256 = hashlib.sha256()
        size = 0
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                size += len(chunk)
                sha256.update(chunk)
        return {"size_bytes": size, "checksum_sha256": sha256.hexdigest()}


    def send_data(self, file_path, requester_id, connection):
        try:
            meta = self.compute_file_meta(file_path)
            meta_msg = {
                "type": "file_meta",
                "file_name": os.path.basename(file_path),
                "size_bytes": meta["size_bytes"],
                "checksum_sha256": meta["checksum_sha256"],
                "sender_id": self.id,
                "requester_id": requester_id
            }
            connection.sendall((json.dumps(meta_msg) + "\n").encode())
            print(f"{self.id} - Sent file_meta for '{meta_msg['file_name']}' to {requester_id}")

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

    def verify_file_integrity(self, file_name, output_path):
        rebuilt_size = os.path.getsize(output_path)
        print(f"{self.id} - Rebuilt file size: {rebuilt_size} bytes")

        sha256 = hashlib.sha256()
        with open(output_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        rebuilt_hash = sha256.hexdigest()
        print(f"{self.id} - Rebuilt file checksum: {rebuilt_hash[:16]}...")

        original_meta = self.received_meta.get(file_name)
        if original_meta:
            if rebuilt_hash == original_meta["checksum_sha256"] and rebuilt_size == original_meta["size_bytes"]:
                print(f"{self.id} - Verificação OK! Arquivo íntegro e tamanho correto.")
            else:
                print(f"{self.id} - Falha na verificação!")
                if rebuilt_hash != original_meta["checksum_sha256"]:
                    print("   - Checksum diferente.")
                if rebuilt_size != original_meta["size_bytes"]:
                    print(f"   - Tamanho incorreto: {rebuilt_size} / esperado {original_meta['size_bytes']}")
        else:
            print(f"{self.id} - Metadados originais não disponíveis para verificação completa.")
    

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
        
        self.verify_file_integrity(file_name, output_path)

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

                    elif data["type"] == "file_meta" and data["requester_id"] == self.id:
                        file_name = data["file_name"]
                        self.received_meta[file_name] = {
                            "size_bytes": data["size_bytes"],
                            "checksum_sha256": data["checksum_sha256"]
                        }
                        print(f"{self.id} - Received file_meta for '{file_name}' (size={data['size_bytes']}, checksum={data['checksum_sha256'][:16]}...)")

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

def create_files(name, size):
    path = os.path.join("files/A", name)

    content = os.urandom(size)

    with open(path, "wb") as f:
        f.write(content)

def generate_test_2_peers():
    peers = {
        "A": Peer("0.0.0.0", 8001, "A"),
        "B": Peer("0.0.0.0", 8002, "B"),
    }

    os.mkdir("files/A")
    os.mkdir("files/B")

    create_files("pequeno", 10 * 1024)
    create_files("medio", 1024 * 1024)
    create_files("grande", 10 * 1024 * 1024)

    return peers

def generate_test_4_peers():
    peers = {
        "A": Peer("0.0.0.0", 8001, "A"),
        "B": Peer("0.0.0.0", 8002, "B"),
        "C": Peer("0.0.0.0", 8003, "C"),
        "D": Peer("0.0.0.0", 8004, "D")
    }

    os.mkdir("files/A")
    os.mkdir("files/B")
    os.mkdir("files/C")
    os.mkdir("files/D")

    create_files("pequeno", 20 * 1024)
    create_files("medio", 5 * 1024 * 1024)
    create_files("grande", 20 * 1024 * 1024)

    return peers

if __name__ == "__main__":
    test_id = input("Digite 2 para executar o teste com 2 peers ou 4 para executar o teste com 4 peers: ")
    
    if (test_id == "2"):
        peers = generate_test_2_peers()
    else:
        if (test_id != "4"):
            print("Inválido. Assumindo teste com 4 peers.")
        peers = generate_test_4_peers()

    for peer in peers.values():
        peer.start()

    time.sleep(2)

    for peer in peers.values():
        for otherPeer in peers.values():
            if peer.port < otherPeer.port:
                peer.connect("127.0.0.1", otherPeer.port)
                time.sleep(0.1)

    try:       
        while True:
            peer_id = input("Digite o id do Peer que vai fazer a requisição: ")
            file_name = input("Qual o nome do arquivo (pequeno, medio ou grande): ")
            peers[peer_id].request_data(file_name)
    except KeyboardInterrupt:
        shutil.rmtree("files/A")
        shutil.rmtree("files/B")
        shutil.rmtree("files/C")
        shutil.rmtree("files/D")
