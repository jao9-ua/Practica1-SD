import sys
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

        try:
            while True:
                client_socket, client_address = server.accept()
                threading.Thread(target=self.handle_client, args=(client_socket,)).start()
        except KeyboardInterrupt:
            print("Shutting down server.")
            server.close()

    def handle_client(self, client_socket):
        try:
            while True:
                message = client_socket.recv(1024).decode('utf-8')
                if message:
                    print(f"Received message: {message}")
                    self.process_message(client_socket, message)
        except ConnectionResetError:
            print("Client disconnected unexpectedly.")
        finally:
            client_socket.close()

    def process_message(self, client_socket, message):
        try:
            message_data = json.loads(message)
        except json.JSONDecodeError:
            print("Failed to decode JSON.")
            return

        action = message_data.get('action')

        if action == 'register':
            drone_id = message_data['drone_id']
            if drone_id in self.drones:
                response = {'status': 'error', 'message': 'Drone already registered.'}
            else:
                token = str(uuid.uuid4())
                self.drones[drone_id] = {'socket': client_socket, 'token': token, 'drone_id': drone_id}
                response = {'status': 'registered', 'token': token}
                self.write_to_file(token, drone_id)
            client_socket.send(json.dumps(response).encode('utf-8'))

    def write_to_file(self, token, drone_id):
        with open('drones_tokens.csv', 'a') as file:
            file.write(f'{drone_id}, {drone_id}, {token}\n')

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python AD_Registry.py <host> <port>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])

    registry = Registry(host=host, port=port)
    registry.start()
