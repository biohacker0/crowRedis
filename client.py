import socket

class RedisClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.in_transaction = False

    def connect(self):
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((self.host, self.port))
        print(f"Connected to Redis server at {self.host}:{self.port}")

    def send_command(self, command):
        self.client_socket.send(command.encode('utf-8'))

    def receive_response(self):
        response = self.client_socket.recv(1024).decode('utf-8')
        return response

    def close(self):
        self.client_socket.close()
        print("Connection closed")

if __name__ == "__main__":
    client = RedisClient('127.0.0.1', 6381)
    client.connect()

    while True:
        command = input("Enter Redis command (or 'exit' to quit): ").strip()

        if command.lower() == 'exit':
            break

        if command.upper() == 'MULTI':
            client.in_transaction = True
            print("Entered transaction mode. Enter transaction commands.")

            transaction_commands = []
            while True:
                transaction_command = input("Transaction command (or 'EXEC'/'DISCARD' to end transaction): ").strip()
                if transaction_command.upper() in ['EXEC', 'DISCARD']:
                    break
                transaction_commands.append(transaction_command)

            for transaction_command in transaction_commands:
                client.send_command(transaction_command)
                response = client.receive_response()
                print(response)

            client.in_transaction = False
        else:
            if command.startswith("SET") and "EX" in command.upper():
                # SET command with TTL
                client.send_command(command)
                response = client.receive_response()
            elif command.startswith("TTL"):
                # TTL command
                client.send_command(command)
                response = client.receive_response()
                print(response)
            elif command.startswith("INCR") or command.startswith("DECR"):
                # INCR or DECR command
                client.send_command(command)
                response = client.receive_response()
                print(response)
            else:
                if client.in_transaction:
                    print("Transaction commands are not sent until 'EXEC' is issued.")
                else:
                    client.send_command(command)
                    response = client.receive_response()
                    print(response)

    client.close()



