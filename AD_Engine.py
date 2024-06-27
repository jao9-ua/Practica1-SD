import socket
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer
import csv

class Engine:
    def __init__(self, host, port, max_drones, weather_host, weather_port, topic_consumer, topic_producer, broker_host, broker_port):
        self.host = host
        self.port = port
        self.max_drones = max_drones
        self.weather_host = weather_host
        self.weather_port = weather_port
        self.drones = {}
        self.figures = []
        self.load_figures()
        self.topic_consumer = topic_consumer
        self.topic_producer = topic_producer
        self.producer = KafkaProducer(
            bootstrap_servers=[f'{broker_host}:{broker_port}'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            self.topic_consumer,
            bootstrap_servers=[f'{broker_host}:{broker_port}'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='engine'
        )

    def load_figures(self):
        with open('figuras.txt', 'r') as file:
            data = file.read()
            self.figures = json.loads(data)

    def send_message(self, topic, drone_id, message):
        message.update({'drone_id': drone_id})
        self.producer.send(topic, value=message)
        self.producer.flush()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen(5)
        print(f"Engine started on {self.host}:{self.port}")

        threading.Thread(target=self.check_weather).start()
        threading.Thread(target=self.listen_to_drones).start()

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
        if action == 'authenticate':
            if len(self.drones) < self.max_drones:
                drone_id = message_data['drone_id']
                stored_drones = self.read_drones_from_csv()
                if stored_drones[drone_id] == message['token']:
                    self.drones[drone_id] = {'socket': client_socket, 'last_seen': time.time()}
                    response = {'status': 'REGISTERED'}
                    client_socket.send(json.dumps(response).encode('utf-8'))
                else:
                    response = {'status': 'UNAUTHORIZED'}
                    client_socket.send(json.dumps(response).encode('utf-8'))
            else:
                response = {'status': ''}
                client_socket.send(json.dumps(response).encode('utf-8'))
        elif action == 'start':
            self.start_figure()
        elif action == 'move':
            drone_id = message_data['drone_id']
            position = message_data['position']
            self.drones[drone_id]['position'] = position
            self.broadcast_state()

    def start_figure(self):
        for figure in self.figures:
            for command in figure['commands']:
                drone_id = command['drone_id']
                position = command['position']
                self.drones[drone_id]['position'] = position
                self.send_message(self.topic, drone_id, {'action': 'move', 'position': position})
                time.sleep(1)
            time.sleep(5)

    def broadcast_state(self):
        state = {'drones': {drone_id: data['position'] for drone_id, data in self.drones.items()}}
        for drone_socket in self.drones.values():
            drone_socket['socket'].send(json.dumps(state).encode('utf-8'))

    def listen_to_drones(self):
        for message in self.consumer:
            drone_id = message.value['drone_id']
            action = message.value.get('action')
            if action == 'arrived':
                self.drones[drone_id]['last_seen'] = time.time()
            self.check_drones_status()

    def check_drones_status(self):
        current_time = time.time()
        for drone_id, drone_data in list(self.drones.items()):
            if current_time - drone_data['last_seen'] > 10:
                print(f"Drone {drone_id} is absent, unregistering.")
                del self.drones[drone_id]

    def check_weather(self):
        while True:
            weather_data = self.get_weather()
            if weather_data['temperature'] < 0:
                self.stop_show()
            time.sleep(60)

    def get_weather(self):
        weather_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        weather_socket.connect((self.weather_host, self.weather_port))
        weather_socket.send(json.dumps({'action': 'get_weather'}).encode('utf-8'))
        weather_data = json.loads(weather_socket.recv(1024).decode('utf-8'))
        weather_socket.close()
        return weather_data

    def stop_show(self):
        for drone_socket in self.drones.values():
            drone_socket['socket'].send(json.dumps({'action': 'stop'}).encode('utf-8'))

    def read_drones_from_csv(self):
        drones = {}
        with open("drones_token.csv", mode='r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                drone_id = row['drone_id']
                token = row['token']
                drones[drone_id] = token
        return drones

if __name__ == '__main__':
    engine = Engine(host='127.0.0.1',
                    port=8000,
                    max_drones=10,
                    weather_host='127.0.0.1',
                    weather_port=9000,
                    topic_producer="drone_command",
                    topic_consumer="drone_answer",
                    broker_host="localhost",
                    broker_port=9092)
    engine.start()
