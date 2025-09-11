import socket
import threading
import random
import time
from clickhouse_connect import get_client
from pathlib import Path
import json

# Server address
HOST = 'localhost'
PORT = 9001
sum_latency = 0.000
sum_client = 0
latency_list = []

BASE_DIR = Path(__file__).parent
QUERY_PATH = BASE_DIR / 'query_scenarios' / 'queries.sql'

# Simulate a single client
def simulate_client(client_id, queries):
    global sum_latency, sum_client, latency_list
    try:
        # Random priority (1–10)
        priority = random.randint(1, 9)

        # Random query
        query = random.choice(queries)

        # Random delay before sending query (0.1 to 2 seconds)
        delay = random.uniform(0.1, 2.0)

        # Connect to server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((HOST, PORT))

            # Send priority
            sock.sendall(f"{priority}\n".encode())

            # Wait before sending query
            time.sleep(delay)

            # Send query
            start_time = time.perf_counter()
            sock.sendall(query.encode())

            # Receive response
            response = sock.recv(16384).decode()
            end_time = time.perf_counter()

            latency = end_time - start_time
            sum_latency += latency
            latency_list.append(latency)
            print(f"[Client {client_id}] Priority: {priority}, Latency: {latency:.3f}s, Query: {query}, Response: {response[:60]}...")
            sock.close()
            sum_client += 1;
            print(f"[Client {client_id}] Connection closed. {sum_client}")

    except Exception as e:
        print(f"[Client {client_id}] Error: {e}")

def load_queries(path):
    with open(path, 'r') as f:
        raw = f.read()

    # Split by query markers
    raw_queries = raw.split('\n\n')
    queries = []

    for raw in raw_queries:
        lines = raw.strip().splitlines()
        sql_lines = [line for line in lines if not line.startswith('--') and line.strip()]
        if sql_lines:
            queries.append('\n'.join(sql_lines))

    return queries

# Launch 100 clients
def launch_clients(queries, num_clients=5):
    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=simulate_client, args=(i, queries))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    print(f"🚀 latency={sum_latency:.3f}")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((HOST, PORT))
        priority = 0
        sock.sendall(f"{priority}\n".encode())
        time.sleep(0.5)
        data_str = json.dumps(latency_list)
        sock.sendall(data_str.encode())
        time.sleep(10)
        sock.close()


if __name__ == "__main__":
    queries = load_queries(QUERY_PATH)
    launch_clients(queries)
