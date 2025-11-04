import socket
import threading
import time
import json

class Peer:
    def __init__(self, host, port, id, datas={}):
        self.host = host
        self.port = port
        self.id = id
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connections = []
        self.datas = datas

    def connect(self, peer_host, peer_port):
        connection = socket.create_connection((peer_host, peer_port))
        self.connections.append(connection)
        print(f"Connected to {peer_host}:{peer_port}")
        threading.Thread(target=self.handle_client, args=(connection, (peer_host, peer_port))).start()


    def listen(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        print(f"Listening for connections on {self.host}:{self.port}")

        while True:
            connection, address = self.socket.accept()
            self.connections.append(connection)
            print(f"Accepted connection from {address}")
            threading.Thread(target=self.handle_client, args=(connection, address)).start()

    def send_data(self, data_id, connection):
        try:
            request = {
                "type": "send",
                "id": data_id, 
                "content": self.datas[data_id]
            }
            
            request = json.dumps(request)
            
            connection.sendall((request + "\n").encode())
        except socket.error as e:
            print(f"Failed to send data. Error: {e}")
            self.connections.remove(connection)

    def handle_client(self, connection, address):
        buffer = ""
        while True:
            try:
                chunk = connection.recv(1024)
                if not chunk:
                    break

                buffer += chunk.decode()

                # Processa todas as mensagens completas no buffer
                while "\n" in buffer:
                    msg, buffer = buffer.split("\n", 1)
                    if not msg.strip():
                        continue

                    data = json.loads(msg)

                    # Trata mensagens recebidas
                    if data["type"] == "request":
                        data_id = data["data_id"]
                        if data_id in self.datas:
                            self.send_data(data_id, connection)

                    elif data["type"] == "send":
                        data_id = data["id"]
                        self.datas[data_id] = data["content"]
                        print(f"{self.id} - Received data {data_id} from {address}: '{data['content']}'")

            except (socket.error, json.JSONDecodeError) as e:
                break

        print(f"Connection from {address} closed.")
        if connection in self.connections:
            self.connections.remove(connection)
        connection.close()


    def start(self):
        listen_thread = threading.Thread(target=self.listen)
        listen_thread.start()
        
    def request_data(self, data_id):
        print(f"Requesting data {data_id} from peers.")
        request = {
            "type": "request",
            "data_id": data_id
        }
        
        request = json.dumps(request)
        
        for connection in self.connections:
            connection.sendall((request + "\n").encode())

# Example usage:
if __name__ == "__main__":
    datas = {
        1: "Mensagem 1",
        2: "Mensagem 2",
        3: "Mensagem 3",
        4: "Mensagem 4",
    }
    
    peers = {
        "A": Peer("0.0.0.0", 8001, "A"),
        "B": Peer("0.0.0.0", 8002, "B", ({1: datas[1]})),
        "C": Peer("0.0.0.0", 8003, "C"),
        "D": Peer("0.0.0.0", 8004, "D"),
    }
    
    for peer in peers.values():
        peer.start()
    
    time.sleep(2)    
      
    for peer in peers.values():
        for otherPeer in peers.values():
            if peer.id != otherPeer.id:
                peer.connect("127.0.0.1", otherPeer.port)
                time.sleep(0.1)
                
    for peer in peers.values():
        print(f"Peer ID: {peer.id} - {peer.datas}")
        
    while True:
        peer_id = input("Digite o id do Peer para receber a mensagem: ")
        
        if peer_id == "0":
            break
        
        data_id = int(input("Qual o id da mensagem: "))
        
        peers[peer_id].request_data(data_id)
        
    for peer in peers.values():
        print(f"Peer ID: {peer.id} - {peer.datas}")