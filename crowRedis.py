import socket
import threading
import time

class RedisServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = {}  # Data store for key-value pairs
        self.lock = threading.Lock()
        self.snapshot_interval = 60  # Snapshot interval in seconds
        self.last_snapshot_time = time.time()
        self.aof_filename = 'redis_aof.log'
        self.load_aof()
        self.in_transaction = False
        self.transaction_commands = []
        self.aof_enabled = False

        # Initialize with loading data from snapshot file
        self.load_snapshot()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            print(f"Server listening on {self.host}:{self.port}")

            while True:
                try:
                    client_socket, client_address = server_socket.accept()
                    print(f"Accepted connection from {client_address[0]}:{client_address[1]}")
                    threading.Thread(target=self.handle_client, args=(client_socket,)).start()
                except Exception as e:
                    print(f"Error accepting client connection: {e}")

    def handle_client(self, client_socket):
        try:
            while True:
                request = client_socket.recv(1024).decode('utf-8')
                if not request:
                    break

                parts = request.strip().split()
                command = parts[0].upper()

                if command == "SET":
                    self.handle_set(client_socket, parts)
                elif command == "GET":
                    self.handle_get(client_socket, parts)
                elif command == "DEL":
                    self.handle_del(client_socket, parts)
                elif command == "SAVE":
                    self.handle_save(client_socket)
                elif command == "MULTI":
                    self.handle_transaction(client_socket)
                elif command == "LPUSH":
                    self.handle_lpush(client_socket, parts)
                elif command == "RPUSH":
                    self.handle_rpush(client_socket, parts)
                elif command == "LPOP":
                    self.handle_lpop(client_socket, parts)
                elif command == "RPOP":
                    self.handle_rpop(client_socket, parts)
                elif command == "LRANGE":
                    self.handle_lrange(client_socket, parts)
                else:
                    client_socket.send(b"Invalid command\n")

                # Check if it's time to create a snapshot
                if time.time() - self.last_snapshot_time >= self.snapshot_interval:
                    self.save_snapshot()
                    self.last_snapshot_time = time.time()
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
            
############# basic stuff to set,get, delete data from RAM, bit simlistic for now. 

    def handle_set(self, client_socket, parts):
        if len(parts) >= 3:
            key, value = parts[1], ' '.join(parts[2:])
            with self.lock:
                self.data[key] = value
                self.append_to_aof(f"SET {key} {value}")
            client_socket.send(b"OK\n")
        else:
            client_socket.send(b"Invalid SET command\n")

    def handle_get(self, client_socket, parts):
        if len(parts) == 2:
            key = parts[1]
            with self.lock:
                value = self.data.get(key, "nil")
            client_socket.send(f"{value}\n".encode('utf-8'))
        else:
            client_socket.send(b"Invalid GET command\n")

    def handle_del(self, client_socket, parts):
        if len(parts) == 2:
            key = parts[1]
            with self.lock:
                if key in self.data:
                    del self.data[key]
                    self.append_to_aof(f"DEL {key}")
                    client_socket.send(b"1\n")  # Key deleted successfully
                else:
                    client_socket.send(b"0\n")  # Key not found
        else:
            client_socket.send(b"Invalid DEL command\n")
            
########## this are is for our persistance funtionality , like snapshot and AOF ,big bois stuff hehe

    def handle_save(self, client_socket):
        with self.lock:
            self.save_snapshot()
        client_socket.send(b"Data saved to snapshot file\n")

    def save_snapshot(self):
        with open('redis_snapshot.txt', 'w') as snapshot_file:
            for key, value in self.data.items():
                snapshot_file.write(f"SET {key} {value}\n")

    def load_snapshot(self):
        try:
            with open('redis_snapshot.txt', 'r') as snapshot_file:
                for line in snapshot_file:
                    parts = line.strip().split()
                    if len(parts) >= 3 and parts[0] == "SET":
                        key, value = parts[1], ' '.join(parts[2:])
                        self.data[key] = value
        except FileNotFoundError:
            pass

    def load_aof(self):
        try:
            with open(self.aof_filename, 'r') as aof_file:
                commands = aof_file.readlines()
                for command in commands:
                    self.handle_command(command.strip())
        except FileNotFoundError:
            pass

    def handle_command(self, command):
        parts = command.strip().split()
        cmd = parts[0].upper()

        if cmd == "SET":
            key, value = parts[1], ' '.join(parts[2:])
            with self.lock:
                self.data[key] = value
        elif cmd == "DEL":
            key = parts[1]
            with self.lock:
                if key in self.data:
                    del self.data[key]

    def enable_aof(self):
        self.aof_enabled = True

    def disable_aof(self):
        self.aof_enabled = False

    def append_to_aof(self, command):
        if self.aof_enabled:
            with open(self.aof_filename, 'a') as aof_file:
                aof_file.write(command + '\n')

    def recover_from_aof(self):
        if self.aof_enabled:
            try:
                with open(self.aof_filename, 'r') as aof_file:
                    for line in aof_file:
                        self.handle_command(line)

            except FileNotFoundError:
                pass
            
################################ ayo, this is to handle those complex transactions, dont you dare mess this up 

    def handle_transaction(self, client_socket):
        self.in_transaction = True
        self.transaction_commands = []

        while True:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break

            parts = request.strip().split()
            command = parts[0].upper()

            if command == "EXEC":
                print("Received EXEC command")
                # Execute the transaction commands
                result = self.execute_transaction()
                client_socket.send(result.encode('utf-8'))
                print(f"Sent result: {result}")
                self.in_transaction = False
            elif command == "DISCARD":
                print("Received DISCARD command")
                # Discard the transaction
                self.transaction_commands = []
                client_socket.send(b"OK\n")
                print("Sent OK response")
                self.in_transaction = False
            elif command in ["LPUSH", "RPUSH", "LPOP", "RPOP"]:
                print(f"Received transaction command: {request}")
                # Add the command to the transaction
                self.transaction_commands.append(request)
            else:
                return "ERROR: Transaction contains unsupported commands\n"

    def execute_transaction(self):
        if not self.in_transaction:
            return "NO TRANSACTION\n"

        result = ""
        with self.lock:
            for command in self.transaction_commands:
                parts = command.strip().split()
                cmd = parts[0].upper()

                if cmd == "SET":
                    key, value = parts[1], ' '.join(parts[2:])
                    self.data[key] = value
                elif cmd == "GET":
                    key = parts[1]
                    value = self.data.get(key, "nil")
                    result += f"{value}\n"
                elif cmd == "DEL":
                    key = parts[1]
                    if key in self.data:
                        del self.data[key]
                elif cmd == "LPUSH":
                    key, *values = parts[1:]
                    if key not in self.data:
                        self.data[key] = []
                    self.data[key] = values + self.data[key]
                elif cmd == "RPUSH":
                    key, *values = parts[1:]
                    if key not in self.data:
                        self.data[key] = []
                    self.data[key].extend(values)
                elif cmd == "LPOP":
                    key = parts[1]
                    if key in self.data and self.data[key]:
                        popped_value = self.data[key].pop(0)
                        result += f"{popped_value}\n"
                elif cmd == "RPOP":
                    key = parts[1]
                    if key in self.data and self.data[key]:
                        popped_value = self.data[key].pop()
                        result += f"{popped_value}\n"
                else:
                    return "ERROR: Transaction contains unsupported commands\n"

        self.transaction_commands = []
        return result
    
    ##################### funtions for LPUSH,RPUSH,LPOP,RPOP,LRANGE with flages , ^^w^^

    def handle_lpush(self, client_socket, parts):
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            with self.lock:
                if key not in self.data:
                    self.data[key] = []
                self.data[key] = values + self.data[key]
                self.append_to_aof(f"LPUSH {key} {' '.join(values)}")
            client_socket.send(b"OK\n")
        else:
            client_socket.send(b"Invalid LPUSH command\n")

    def handle_rpush(self, client_socket, parts):
        if len(parts) >= 3:
            key = parts[1]
            values = parts[2:]
            with self.lock:
                if key not in self.data:
                    self.data[key] = []
                self.data[key].extend(values)
                self.append_to_aof(f"RPUSH {key} {' '.join(values)}")
            client_socket.send(b"OK\n")
        else:
            client_socket.send(b"Invalid RPUSH command\n")

    def handle_lpop(self, client_socket, parts):
        if len(parts) == 2:
            key = parts[1]
            with self.lock:
                if key in self.data and self.data[key]:
                    popped_value = self.data[key].pop(0)
                    self.append_to_aof(f"LPOP {key}")
                    client_socket.send(f"{popped_value}\n".encode('utf-8'))
                else:
                    client_socket.send(b"nil\n")
        else:
            client_socket.send(b"Invalid LPOP command\n")

    def handle_rpop(self, client_socket, parts):
        if len(parts) == 2:
            key = parts[1]
            with self.lock:
                if key in self.data and self.data[key]:
                    popped_value = self.data[key].pop()
                    self.append_to_aof(f"RPOP {key}")
                    client_socket.send(f"{popped_value}\n".encode('utf-8'))
                else:
                    client_socket.send(b"nil\n")
        else:
            client_socket.send(b"Invalid RPOP command\n")

    def handle_lrange(self, client_socket, parts):
        if len(parts) == 4:
            key = parts[1]
            start = int(parts[2])
            stop = int(parts[3])
            with self.lock:
                if key in self.data and isinstance(self.data[key], list):
                    values = self.data[key][start:stop+1]
                    result = ' '.join(map(str, values))
                    client_socket.send(f"{result}\n".encode('utf-8'))
                else:
                    client_socket.send(b"Invalid LRANGE command\n")
        else:
            client_socket.send(b"Invalid LRANGE command\n")

if __name__ == "__main__":
    redis_server = RedisServer('127.0.0.1', 6381)
    redis_server.enable_aof()  # Enable AOF for logging and recovery
    redis_server.recover_from_aof()  # Recover data from the AOF file
    redis_server.start()