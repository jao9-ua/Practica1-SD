import socket
import threading
import json
import uuid


class Registry:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.drones = {}

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Registry started on {self.host}:{self.port}")

        while True:
            client_socket, client_address = server.accept()
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        while True:
            message = client_socket.recv(1024).decode('utf-8')
            if message:
                print(f"Received message: {message}")
                self.process_message(client_socket, message)

    def process_message(self, client_socket, message):
        message_data = json.loads(message)
        action = message_data.get('action')

        if action == 'register':
            drone_id = message_data['drone_id']
            token = str(uuid.uuid4())
            self.drones[drone_id] = {'socket': client_socket, 'token': token, 'drone_id':drone_id}
            response = {'status': 'registered', 'token': token}
            self.write_to_file(token, drone_id)
            client_socket.send(json.dumps(response).encode('utf-8'))

    def write_to_file(self, token, drone_id):
        with open('drones_tokens.csv', 'a') as file:
            file.write(f'{drone_id}, {drone_id}, {token}\n')

if __name__ == '__main__':
    registry = Registry(host='127.0.0.1', port=8001)
    registry.start()
