import socket
import threading
import time
import json

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
            try:
                with open('temperature.txt', 'r') as file:
                    temperature = float(file.read().strip())
                response = {'temperature': temperature}
                client_socket.send(json.dumps(response).encode('utf-8'))
                time.sleep(5)  # Send temperature every 5 seconds
            except Exception as e:
                print(f"Error: {e}")
                break

if __name__ == '__main__':
    weather_server = WeatherServer(host='127.0.0.1', port=9000)
    weather_server.start()
