import socket
import threading
import time
import queue
import logging
import concurrent.futures

logging.basicConfig(filename='server.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisServer:
    def __init__(self, host, port, is_master=True, master_address=None):
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
        self.current_transaction = []
        self.is_master = is_master
        self.master_address = master_address
        self.worker_port = None
        self.connected_workers = []
        self.log_queue = queue.Queue()
        self.worker_socket = None

        if is_master:
        # Start the replication log thread on the master
            self.enable_aof()
            self.recover_from_aof()
            self.start()
        else:
            self.enable_aof()
            self.recover_from_aof()
            # If this server is a worker, connect to the master and start replication
            

        # Initialize with loading data from snapshot file
        self.load_snapshot()

    def start(self):
        replication_thread = None

        if self.is_master:
            replication_thread = threading.Thread(target=self.run_replication_log, name='replication-thread')
            print("master thread started")
        else:
            # If this server is a worker, connect to the master
            try:
                self.worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                master_host, master_port = self.master_address.split(':')
                master_port = int(master_port)
                self.worker_socket.connect((master_host, master_port))
                self.worker_socket.send(b"REGISTER")
                print(f"Connected to master server at {self.master_address}")
                replication_thread = threading.Thread(target=self.replicate_data_from_master, name='worker-replication-thread')
                print("worker thread started")
            except Exception as e:
                print(f"Error connecting to master: {e}")
                # Handle errors, e.g., by setting self.worker_socket to None
                return

        # Start the replication thread
        replication_thread.start()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            print(f"Server listening on {self.host}:{self.port}")

            while True:
                try:
                    client_socket, client_address = server_socket.accept()
                    print(f"Accepted connection from {client_address[0]}:{client_address[1]}")
                    threading.Thread(target=self.handle_client, args=(client_socket,client_address)).start()
                except Exception as e:
                    print(f"Error accepting client connection: {e}")


            

    def handle_client(self, client_socket,client_address):
        try:
            while True:
                request = client_socket.recv(1024).decode('utf-8')
                if not request:
                    break
                if self.is_master and request == "REGISTER":
                    self.handle_worker_registration(client_socket,client_address)
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
            

    def handle_worker_registration(self, client_socket,client_address):
        # Handle worker registration here
        logging.debug("handle_worker_registration called")
        if self.is_master:
            # Add the connected worker socket to the list
            self.connected_workers.append(client_socket)
            logging.debug(f"Slave registered from {client_address[0]}:{client_address[1]}")
            logging.debug(f"Number of connected workers: {len(self.connected_workers)}")
            logging.debug(f"Connected workers: {self.connected_workers}")

    def handle_set(self, client_socket, parts):
        if len(parts) >= 3:
            key, value = parts[1], ' '.join(parts[2:])
            with self.lock:
                self.data[key] = value
                self.append_to_aof(f"SET {key} {value}")
                if self.is_master:
                    log_entry = f"SET {key} {value}"
                    self.log_queue.put(log_entry)
                    logging.debug(f"Queued log entry: {log_entry}")  # Add a debug message
            if client_socket is not None:
                client_socket.send(b"OK\n")
        else:
            if client_socket is not None:
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

################################################Transaction Part ####################################################

    def handle_transaction(self, client_socket):
        if self.in_transaction:
            client_socket.send(b"ERROR: Nested transactions are not supported\n")
            return

        self.in_transaction = True
        self.transaction_commands = []
        self.current_transaction = []  # Initialize the current transaction

        client_socket.send(b"OK\n")

        while True:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break

            parts = request.strip().split()
            command = parts[0].upper()

            if command == "EXEC":
                print("Received EXEC command")
                result = self.execute_transaction()
                if result != "ERROR: Transaction contains unsupported commands\n":
                    client_socket.send(result.encode('utf-8'))
                    print(f"Sent result: {result}")
                else:
                    self.transaction_commands = []
                    self.current_transaction = []
                    client_socket.send(b"ERROR: Transaction failed and discarded\n")
                    self.in_transaction = False
                return
            elif command == "DISCARD":
                print("Received DISCARD command")
                self.transaction_commands = []
                self.current_transaction = []
                client_socket.send(b"OK\n")
                self.in_transaction = False
                return
            elif command in ["LPUSH", "RPUSH", "LPOP", "RPOP"]:
                print(f"Received transaction command: {request}")
                self.transaction_commands.append(request)
                self.current_transaction.append(request)
            else:
                client_socket.send(b"ERROR: Transaction contains unsupported commands\n")

    def execute_transaction(self,client_socket):
        if not self.in_transaction:
            return "NO TRANSACTION\n"

        result = ""
        with self.lock:
            for command in self.transaction_commands:
                parts = command.strip().split()
                cmd = parts[0].upper()

                if cmd == "DISCARD":
                # Rollback the current transaction and discard it
                    self.transaction_commands = []
                    self.current_transaction = []  # Add this line
                    client_socket.send(b"DISCARDED\n")
                    self.in_transaction = False
                    return
                elif cmd == "SET":
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
        self.current_transaction = []  # Clear current transaction
        return result

####################################LPUSH,LPOP,RPUSH,RPOP etc part####################################################################

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

##############################################The distrtibuted computing Data-replication part##################################

    def run_replication_log(self):
        while True:
            try:
                if self.log_queue.empty() == False:
                    logging.debug(f"Queued log entry: run_replication_log ran")
                    log_entry = self.log_entry = self.log_queue.get()  # Get log entry from the queue with a 
                    logging.debug(f"log entry data 392:{log_entry}")
                    self.send_replication_log(log_entry)  # Send the log entry to connected worker
                    logging.debug(f"sent data to send_replication_log funtion")
            except Exception as E:
                logging.debug(f"Queue entry 390:{E}")


    def send_replication_log(self, log_entry):
        logging.debug("send_replication_log called")
        logging.debug(f"Number of connected workers: {len(self.connected_workers)}")
        logging.debug(f"Connected workers: {self.connected_workers}")

        # Send the replication log data to connected workers
        for worker_socket in self.connected_workers:
            try:
                logging.debug(f"Sending data to worker: {log_entry}")
                worker_socket.send(log_entry.encode('utf-8'))
                logging.debug("Data sent to worker successfully")
            except Exception as e:
                logging.error(f"Error sending replication data to worker: {e}")


    def replicate_data_from_master(self):
        logging.debug("replicate_data_from_master called")
        while True:
            try:
                data = self.worker_socket.recv(1024).decode('utf-8')
                if data:
                    logging.debug(f"Received data from master: {data}")
                    # Process the received data and apply replication
                    self.apply_replication_data(data)
                else:
                    logging.debug("No data received from master")
            except ConnectionResetError:
                logging.error("Connection with master reset. Reconnecting...")
                
            except Exception as e:
                logging.debug(f"Slave socket - {self.worker_socket}" )
                logging.error(f"Error replicating data from master: {e}") # Stop replication thread if an error occurs
                return


    def apply_replication_data(self, data):
        logging.debug(f"apply_replication_data funtion called 437:: {data}")
        parts = data.strip().split()
        command = parts[0].upper()

        if command == "SET":
            self.handle_set(None, parts)
           
                
        # Handle other replication commands as needed

if __name__ == "__main__":
    is_master = input("Are you running as a master (y/n)? ").lower() == 'y'

    if is_master:
        host = '127.0.0.1'     # This is the folder-1 where I am running this script as master. ok :3
        port = 6381
        master_address = None
        worker_port = None
        redis_server = RedisServer(host, port, is_master, master_address)
    else:
        master_address = '127.0.0.1:6381'
        host = '127.0.0.1'
        worker_port = 6382
        redis_server = RedisServer(host, worker_port, is_master, master_address)
        
    redis_server.start()   # In terminal you can see three connection, one form client and 2 others of worker-1 worker-2 instances


