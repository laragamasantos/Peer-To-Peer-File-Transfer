import socket
import threading
import time
import json

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

    def connect(self, peer_host, peer_port):
        connection = socket.create_connection((peer_host, peer_port))
        self.connections.append(connection)
        # print(f"Connected to {peer_host}:{peer_port}")
        threading.Thread(target=self.handle_client, args=(connection, (peer_host, peer_port)), daemon=True).start()


    def listen(self):
        self.socket.bind((self.host, self.port))
        self.socket.listen(10)
        # print(f"Listening for connections on {self.host}:{self.port}")

        while True:
            connection, address = self.socket.accept()
            self.connections.append(connection)
            # print(f"Accepted connection from {address}")
            threading.Thread(target=self.handle_client, args=(connection, address), daemon=True).start()

    def send_data(self, data_id, requester_id, connection):
        try:
            request = {
                "type": "send",
                "data_id": data_id, 
                "content": self.datas[data_id],
                "requester_id": requester_id,
                "sender_id": self.id
            }
            
            request = json.dumps(request)
            
            connection.sendall((request + "\n").encode())
            print(f"{self.id} - Send data {data_id} to {requester_id}")
        except socket.error as e:
            print(f"Failed to send data. Error: {e}")
            self.connections.remove(connection)
            
    def request_data(self, data_id):
        print(f"Requesting data {data_id} from peers.")
        request = {
            "type": "request",
            "data_id": data_id,
            "requester_id": self.id
        }
        
        request = json.dumps(request)
        
        for connection in self.connections:
            connection.sendall((request + "\n").encode())

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
                    
                    data_id = data["data_id"]
                    # Trata mensagens recebidas
                    if data["type"] == "request":
                        if data_id in self.datas:
                            self.send_data(data_id, data["requester_id"], connection)
                    
                    elif (data["type"] == "send") and (data["requester_id"] == self.id) and (data_id not in self.datas):          
                        self.datas[data_id] = data["content"]
                        print(f"{self.id} - Received data {data['data_id']} from {data['sender_id']}: '{data['content']}'")

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
            if peer.port < otherPeer.port:
                peer.connect("127.0.0.1", otherPeer.port)
                time.sleep(0.1)
                
    print("Estado inicial:")
    for peer in peers.values():
        print(f"Peer ID: {peer.id} - {peer.datas}")
        
    while True:
        peer_id = input("Digite o id do Peer para receber a mensagem: ")
        
        if peer_id == "0":
            break
        
        data_id = int(input("Qual o id da mensagem: "))
        
        peers[peer_id].request_data(data_id)
        
        print("\nEstado atual:")
        for peer in peers.values():
            print(f"Peer ID: {peer.id} - {peer.datas}")
        print("\n")