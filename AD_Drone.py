import socket
import json
import time
import sys
import networkx as nx
from kafka import KafkaConsumer, KafkaProducer

class Drone:
    def __init__(self, drone_id, engine_ip, engine_port, registry_ip, registry_port, broker_ip, broker_port, topic_consumer, topic_producer):
        self.drone_id = drone_id
        self.engine_ip = engine_ip
        self.engine_port = engine_port
        self.registry_ip = registry_ip
        self.registry_port = registry_port
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.topic_consumer = topic_consumer
        self.topic_producer = topic_producer
        self.token = None
        self.position = [1, 1]  # Default starting position

    def start(self):
        self.register()
        if self.token:
            self.authenticate_to_engine()
            self.listen_to_kafka()

    def register(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.registry_ip, self.registry_port))
                registration_message = json.dumps({'action': 'register', 'drone_id': self.drone_id})
                sock.sendall(registration_message.encode('utf-8'))
                response = sock.recv(1024).decode('utf-8')
                response_data = json.loads(response)
                if response_data['status'] == 'registered':
                    self.token = response_data['token']
                    print(f"Registered with token: {self.token}")
                else:
                    print("Registration failed. Response:", response_data)
        except Exception as e:
            print(f"Registration error: {e}")

    def authenticate_to_engine(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((self.engine_ip, self.engine_port))
                auth_message = json.dumps({'action': 'authenticate', 'token': self.token})
                sock.sendall(auth_message.encode('utf-8'))
                print("Authenticated with the engine.")
        except Exception as e:
            print(f"Authentication error: {e}")

    def listen_to_kafka(self):
        try:
            consumer = KafkaConsumer(
                self.topic_consumer,
                bootstrap_servers=f'{self.broker_ip}:{self.broker_port}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            producer = KafkaProducer(
                bootstrap_servers=f'{self.broker_ip}:{self.broker_port}',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            for message in consumer:
                self.process_message(message.value, producer)
        except Exception as e:
            print(f"Kafka connection error: {e}")

    def process_message(self, message, producer):
        if message['action'] == 'move' and message['drone_id'] == self.drone_id:
            self.move(message['position'], producer)

    def move(self, target, producer):
        graph = nx.grid_2d_graph(20, 20)  # Create a 20x20 grid
        try:
            path = nx.astar_path(graph, tuple(self.position), tuple(target), heuristic=self.manhattan_distance)
            for next_position in path[1:]:  # Skip the first element as it is the current position
                self.position = next_position
                print(f"Moving to {self.position}")
                producer.send(self.topic_producer, {'action': 'position', 'drone_id': self.drone_id, 'position': self.position})
                producer.flush()
                time.sleep(1)  # Simulate movement delay
        except nx.NetworkXNoPath:
            print("No path to target.")

    @staticmethod
    def manhattan_distance(a, b):
        return abs(a[0] - b[0]) + abs(a[1] - b[1])

if __name__ == '__main__':
    if len(sys.argv) != 8:
        print("Usage: python AD_Drone.py <DroneID> <EngineIP> <EnginePort> <RegistryIP> <RegistryPort> <BrokerIP> <BrokerPort>")
        sys.exit(1)
    
    drone_id = int(sys.argv[1])
    engine_ip = sys.argv[2]
    engine_port = int(sys.argv[3])
    registry_ip = sys.argv[4]
    registry_port = int(sys.argv[5])
    broker_ip = sys.argv[6]
    broker_port = int(sys.argv[7])
    
    drone = Drone(drone_id, engine_ip, engine_port, registry_ip, registry_port, broker_ip, broker_port, 'drone_answer', 'drone_command')
    drone.start()
