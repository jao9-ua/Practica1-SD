import socket
import threading
import time
import json
from confluent_kafka import Producer, Consumer, KafkaError
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
        self.producer = Producer({'bootstrap.servers': f'{broker_host}:{broker_port}'})
        self.consumer = Consumer({
            'bootstrap.servers': f'{broker_host}:{broker_port}',
            'group.id': 'engine',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.topic_consumer])
        self.map = [[' ' for _ in range(20)] for _ in range(20)]

    def load_figures(self):
        with open('AwD_figuras.json', 'r') as file:
            data = json.load(file)
            self.figures = data['figuras']

    def send_message(self, topic, drone_id, message):
        message.update({'drone_id': drone_id})
        self.producer.produce(topic, value=json.dumps(message))
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
            token = message_data.get('token')
            drone_id = self.validate_token(token)
            if drone_id:
                position, target_position = self.assign_position(drone_id)
                self.drones[drone_id] = {'socket': client_socket, 'position': position, 'target_position': target_position}
                response = {'status': 'authenticated', 'position': position, 'target_position': target_position, 'map': self.map}
                client_socket.send(json.dumps(response).encode('utf-8'))
                if len(self.drones) == len(self.figures[0]['drones']):
                    self.broadcast_map()
            else:
                response = {'status': 'error', 'message': 'Invalid token'}
                client_socket.send(json.dumps(response).encode('utf-8'))

    def validate_token(self, token):
        with open('drones_tokens.csv', 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                if row['token'] == token:
                    return row['drone_id']
        return None

    def assign_position(self, drone_id):
        figure = self.figures[0]
        for drone in figure['drones']:
            if drone['id'] == drone_id:
                return (1, 1), (drone['x'], drone['y'])  # Initial position (1,1) to target position

    def check_weather(self):
        while True:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.connect((self.weather_host, self.weather_port))
                    while True:
                        response = sock.recv(1024).decode('utf-8')
                        if response:
                            response_data = json.loads(response)
                            temperature = response_data.get('temperature')
                            print(f"Current temperature: {temperature}")
            except Exception as e:
                print(f"Weather check error: {e}")
            time.sleep(5)  # Check every 5 seconds

    def listen_to_drones(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    continue
            message = json.loads(msg.value().decode('utf-8'))
            self.process_drone_message(message)

    def process_drone_message(self, message):
        drone_id = message.get('drone_id')
        action = message.get('action')
        if action == 'position':
            position = message.get('position')
            self.update_map(drone_id, position)
            print(f"Drone {drone_id} moved to {position}")
            self.broadcast_map()
        elif action == 'complete':
            print(f"Drone {drone_id} completed its task")

    def update_map(self, drone_id, position):
        for i in range(20):
            for j in range(20):
                if self.map[i][j] == drone_id:
                    self.map[i][j] = ' '
        self.map[position[0]][position[1]] = drone_id

    def broadcast_map(self):
        for drone_id in self.drones:
            self.send_message(self.topic_producer, drone_id, {'action': 'update_map', 'map': self.map})

if __name__ == '__main__':
    engine = Engine(
        host='127.0.0.1',
        port=8000,
        max_drones=10,
        weather_host='127.0.0.1',
        weather_port=9000,
        topic_producer="drone_command",
        topic_consumer="drone_answer",
        broker_host="localhost",
        broker_port=9092
    )
    engine.start()
