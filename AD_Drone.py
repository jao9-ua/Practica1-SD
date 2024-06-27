import socket
import threading
import networkx as nx
import json
import time
from kafka import KafkaConsumer, KafkaProducer

class Drone:
    def __init__(self, drone_id, engine_host, engine_port, registry_host, registry_port, retryer, wait_if_fail, broker_host, broker_port, topic_consumer, topic_producer):
        self.drone_id = drone_id
        self.engine_host = engine_host
        self.engine_port = engine_port
        self.engine_socket = None
        self.registry_host = registry_host
        self.registry_port = registry_port
        self.registry_socket = None
        self.token = None
        self.position = [1, 1]
        self.target = [1, 1]
        self.registered = False
        self.retryer = retryer
        self.wait_if_fail = wait_if_fail
        self.obstacles = None
        self.state = "idle"  # Initial state is idle
        self.topic_producer = topic_producer
        self.topic_consumer = topic_consumer
        self.consumer = KafkaConsumer(
            self.topic_consumer,
            bootstrap_servers=[f'{broker_host}:{broker_port}'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=self.drone_id
        )
        self.producer = KafkaProducer(
            bootstrap_servers=[f'{broker_host}:{broker_port}'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def start(self):
        self.register()
        # self.authenticate_to_engine()
        # threading.Thread(target=self.listen_to_kafka).start()

    def register(self, intent_for_connection=1):
        self.registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.registry_socket.connect((self.registry_host, self.registry_port))
            message = json.dumps({'action': 'register', 'drone_id': self.drone_id})
            response = None
            intent_for_send = 1
            while intent_for_send < self.retryer and (response == None or response.get('status') != 'registered'):
                self.registry_socket.send(message.encode('utf-8'))
                response = json.loads(self.registry_socket.recv(1024).decode('utf-8'))
                if response['status'] == 'registered':
                    print(f"Registered: {response}")
                    self.token = response['token']
                else:
                    print(f'Intent: {intent_for_send}, waiting for retrying {self.wait_if_fail} seconds')
                    intent_for_send += 1
                    time.sleep(self.wait_if_fail)
        except Exception as e:
            print(f'Error in Registry connection, intent {intent_for_connection}, retrying in next {self.wait_if_fail} seconds')
            if intent_for_connection < self.retryer:
                intent_for_connection += 1
                time.sleep(self.wait_if_fail)
                self.register(intent_for_connection)

    def authenticate_to_engine(self):
        self.engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.engine_socket.connect((self.engine_host, self.engine_port))
            message = json.dumps({'action': 'authenticate', 'token': self.token, 'drone_id': self.drone_id})
            self.engine_socket.send(message.encode('utf-8'))
            response = json.loads(self.engine_socket.recv(1024).decode('utf-8'))
            if response['status'] == 'REGISTERED':
                print(f"Authenticated to engine: {response}")
                self.registered = True
            elif response['status'] == 'OUT_OF_BOUND':

        except Exception as e:
            print(f'Error in Engine connection, retrying in next {self.wait_if_fail} seconds')
            time.sleep(self.wait_if_fail)
            self.authenticate_to_engine()

    def listen_to_kafka(self):
        for message in self.consumer:
            print(f"Received message from Kafka: {message.value}")
            self.process_message(message.value)

    def process_message(self, message):
        action = message.get('action')
        if action == 'move' and message.get('drone_id') == self.drone_id:
            self.target = message['position']
            if self.state != "moving":
                self.state = "moving"
                self.move()
        elif action == 'stop' and message.get('drone_id') == self.drone_id:
            self.state = "stopped"
            self.stop()

    def move(self):
        graph = nx.grid_2d_graph(20, 20)
        while self.state == "moving" and self.position != self.target:
            try:
                self.position = nx.astar_path(graph, tuple(self.position), tuple(self.target), heuristic=heuristic)[1]
                print(f"Next position: {self.position}")
                self.send_position()
                time.sleep(5)  # Simulate movement delay
            except nx.NetworkXNoPath:
                print("No se encontró un camino.")
                self.state = "idle"
                break

    def send_position(self):
        message = {'action': 'arrived', 'drone_id': self.drone_id, 'position': self.position}
        self.producer.send(self.topic_producer, value=message)
        self.producer.flush()

    def stop(self):
        print("Stopping drone")
        self.state = "idle"
        if self.engine_socket:
            self.engine_socket.close()

def heuristic(a, b):
    # Utiliza la distancia Manhattan como heurística
    return abs(a[0] - b[0]) + abs(a[1] - b[1])

if __name__ == '__main__':
    drone = Drone(
        drone_id=4,
        engine_host='127.0.0.1',
        engine_port=8000,
        registry_host='127.0.0.1',
        registry_port=8001,
        retryer=3,
        wait_if_fail=5,
        broker_host='localhost',
        broker_port=9092,
        topic_producer="drone_answer",
        topic_consumer="drone_command"
    )
    drone.start()
