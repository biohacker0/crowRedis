import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import argparse
import time

# Function to create a PostgreSQL database connections
def create_db_connection(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

# Function to insert data into the test table
def insert_data(conn, num_requests):
    try:
        cursor = conn.cursor()
        start_time = time.time()
        for _ in range(num_requests):
            cursor.execute("INSERT INTO test_table (data) VALUES ('test_data');")
        conn.commit()
        end_time = time.time()
        return end_time - start_time
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        return None

# Function to update data in the test table
def update_data(conn, num_requests):
    try:
        cursor = conn.cursor()
        start_time = time.time()
        for _ in range(num_requests):
            cursor.execute("UPDATE test_table SET data = 'updated_data';")
        conn.commit()
        end_time = time.time()
        return end_time - start_time
    except psycopg2.Error as e:
        print(f"Error updating data: {e}")
        return None

# Function to delete data from the test table
def delete_data(conn, num_requests):
    try:
        cursor = conn.cursor()
        start_time = time.time()
        for _ in range(num_requests):
            cursor.execute("DELETE FROM test_table;")
        conn.commit()
        end_time = time.time()
        return end_time - start_time
    except psycopg2.Error as e:
        print(f"Error deleting data: {e}")
        return None

# Function to benchmark transactions
def benchmark_transactions(conn, num_transactions):
    try:
        cursor = conn.cursor()
        start_time = time.time()
        for _ in range(num_transactions):
            cursor.execute("BEGIN;")
            cursor.execute("INSERT INTO test_table (data) VALUES ('test_data');")
            cursor.execute("UPDATE test_table SET data = 'updated_data';")
            cursor.execute("DELETE FROM test_table;")
            cursor.execute("COMMIT;")
        end_time = time.time()
        total_time = end_time - start_time
        throughput = num_transactions / total_time
        average_response_time = total_time / num_transactions
        return total_time, throughput, average_response_time
    except psycopg2.Error as e:
        print(f"Error performing transactions: {e}")
        return None, None, None

if __name__ == "__main__":
    # Database parameters
    db_params = {
         'dbname': 'ENTER_YOUR_dbName',
         'host': 'ENTER_YOUR_HOST_NAME',
         'user': 'ENTER_YOUR_USERNAME',
         'password': 'ENTER_YOUR_PASS'
    }

    conn = create_db_connection(db_params)

    if conn is not None:
        parser = argparse.ArgumentParser(description="PostgreSQL benchmark")
        parser.add_argument("--num_requests", type=int, default=1000, help="Number of requests to perform")
        parser.add_argument("--num_transactions", type=int, default=100, help="Number of transactions to perform")

        args = parser.parse_args()
        num_requests = args.num_requests
        num_transactions = args.num_transactions

        # Benchmark insertion (CREATE) operation
        create_time = insert_data(conn, num_requests)
        print(f"Benchmark INSERT: {num_requests} requests in {create_time:.4f} seconds")

        # Benchmark update operation
        update_time = update_data(conn, num_requests)
        print(f"Benchmark UPDATE: {num_requests} requests in {update_time:.4f} seconds")

        # Benchmark delete operation
        delete_time = delete_data(conn, num_requests)
        print(f"Benchmark DELETE: {num_requests} requests in {delete_time:.4f} seconds")

        # Benchmark transactions
        total_time, throughput, avg_response_time = benchmark_transactions(conn, num_transactions)
        print(f"Benchmark TRANSACTIONS: {num_transactions} transactions in {total_time:.4f} seconds")
        print(f"Throughput: {throughput:.2f} transactions per second")
        print(f"Average response time: {avg_response_time:.4f} seconds")

        conn.close()
