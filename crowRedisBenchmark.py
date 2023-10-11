import argparse
import socket
import time
import threading
import timeit
from crowRedis import RedisServer

class RedisBenchmark:
    def __init__(self, host, port, num_clients, num_requests, num_transactions, socket_pool_size):
        self.host = host
        self.port = port
        self.num_clients = num_clients
        self.num_requests = num_requests
        self.num_transactions = num_transactions
        self.socket_pool_size = socket_pool_size
        self.clients = []

    def run_benchmark(self):
        for _ in range(self.num_clients):
            client = RedisClient(self.host, self.port, self.num_requests, self.num_transactions)
            self.clients.append(client)

        start_time = time.time()
        threads = []

        for client in self.clients:
            thread = threading.Thread(target=client.run)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        end_time = time.time()
        total_time = end_time - start_time
        total_transactions = self.num_clients * self.num_transactions
        throughput = total_transactions / total_time  # Transactions per second
        average_response_time = total_time / total_transactions  # Average response time per transaction

        print(f"Total time taken: {total_time} seconds")
        print(f"Throughput: {throughput:.2f} transactions per second")
        print(f"Average response time: {average_response_time:.4f} seconds")

class RedisClient:
    def __init__(self, host, port, num_requests, num_transactions):
        self.host = host
        self.port = port
        self.num_requests = num_requests
        self.num_transactions = num_transactions

    def run(self):
        for _ in range(self.num_transactions):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((self.host, self.port))
                self.run_transactions(client_socket)

    def run_transactions(self, client_socket):
        for _ in range(self.num_requests):
            self.send_request(client_socket)

    def send_request(self, client_socket):
        # Implement your request sending logic here
        pass

def create_socket_pool(redis_server, pool_size):
    socket_pool = []
    for _ in range(pool_size):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((redis_server.host, redis_server.port))
        socket_pool.append(client_socket)
    return socket_pool

def benchmark_set(redis_server, socket_pool, num_requests):
    def set_command():
        client_socket = socket_pool.pop()
        request = "SET key value\n"
        client_socket.send(request.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        socket_pool.append(client_socket)
    
    return timeit.timeit(set_command, number=num_requests)

def benchmark_get(redis_server, socket_pool, num_requests):
    def get_command():
        client_socket = socket_pool.pop()
        request = "GET key\n"
        client_socket.send(request.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        socket_pool.append(client_socket)
    
    return timeit.timeit(get_command, number=num_requests)

def benchmark_del(redis_server, socket_pool, num_requests):
    def del_command():
        client_socket = socket_pool.pop()
        request = "DEL key\n"
        client_socket.send(request.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        socket_pool.append(client_socket)
    
    return timeit.timeit(del_command, number=num_requests)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis-like server benchmark")
    parser.add_argument("--host", default="127.0.0.1", help="Server hostname (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6381, help="Server port (default: 6381)")
    parser.add_argument("--num_clients", type=int, default=10, help="Number of concurrent clients")
    parser.add_argument("--num_requests", type=int, default=1000, help="Number of requests per client")
    parser.add_argument("--num_transactions", type=int, default=10, help="Number of transactions per client")
    parser.add_argument("--socket_pool_size", type=int, default=10, help="Socket pool size for SET, GET, DEL benchmark")

    args = parser.parse_args()
    benchmark = RedisBenchmark(args.host, args.port, args.num_clients, args.num_requests, args.num_transactions, args.socket_pool_size)
    benchmark.run_benchmark()

    # Create socket pool for SET, GET, DEL benchmark
    redis_server = RedisServer(args.host, args.port)
    socket_pool = create_socket_pool(redis_server, args.socket_pool_size)

    # Benchmark SET operation
    set_time = benchmark_set(redis_server, socket_pool, args.num_requests)
    print(f"Benchmark SET: {args.num_requests} requests in {set_time:.4f} seconds")

    # Benchmark GET operation
    get_time = benchmark_get(redis_server, socket_pool, args.num_requests)
    print(f"Benchmark GET: {args.num_requests} requests in {get_time:.4f} seconds")

    # Benchmark DEL operation
    del_time = benchmark_del(redis_server, socket_pool, args.num_requests)
    print(f"Benchmark DEL: {args.num_requests} requests in {del_time:.4f} seconds")
