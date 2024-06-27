import socket
import threading
import json
import random

class WeatherServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Weather server started on {self.host}:{self.port}")

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

        if action == 'get_weather':
            temperature = random.uniform(-10, 30)  # Simulación de la temperatura
            response = {'temperature': temperature}
            client_socket.send(json.dumps(response).encode('utf-8'))

if __name__ == '__main__':
    weather_server = WeatherServer(host='127.0.0.1', port=9000)
    weather_server.start()
