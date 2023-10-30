# crowRedis
I built my own small simple memory datastore like redis to learn how it works internally and how databases are built , it has set,get, transactions (multi/exec ),persistence(snapshots, AOF) , concurrency support for all things and concurrent transactions , and lots of data replication if you run the servers in master/worker mode

# [Blog-1](https://corvus-ikshana.hashnode.dev/building-a-simple-redis-like-data-store-crowredis-in-python)
# [Blog-2](https://corvus-ikshana.hashnode.dev/crowredis-data-replication-delving-into-distributed-systems)


## Prerequisites

Before you can run crowRedis and the client, make sure you have the following prerequisites installed on your system:

- Python (3.x recommended)
- Socket library (should be included with Python

# How To use this:
 -  After cloning , run the crowRedis.py file , its the server that will listen to all your requests.
 -  Then run the client.py file , its the client where you can write your queries like ex: set name myname , get name , del name.
 -  rest scripts are for benchmarks and logs and snapshot file will be created automatically

  ## Benchmark Test
 I compared crowRedis and postgressSQL and real Redis again each other for some parameters on same hardware to see what are the test results :
- plz don't that this seriously, cause I am comparing a relational database with a memory database, but I wanted to see how much is a RAM-based database faster a disk-based one.
- Also, real Redis use a very complex mechanism for set,get,del of data and, mine is way too simplistic, that's why it's doing so fast operations.
- I am also learning things and might do some stupid comparisons so plz forgive me , I will learn what I don't know and improve ✌️

| Benchmark | Time taken (seconds) | Database |
|-----------|----------------------|----------|
| INSERT    | 0.1802               | postgresSQL |
| UPDATE    | 1.6753               | postgresSQL |
| DELETE    | 0.2250               | postgresSQL |
| TRANSACTIONS | 0.0680            | postgresSQL |
| Throughput | 1470.95 transactions per second | postgresSQL |
| Average response time | 0.0007 seconds | postgresSQL |



| **Metric**            | **Value**                          | **Database** |
|-----------------------|------------------------------------|--------------|
| Total time taken      | 0.021941661834716797 seconds       | crowRedis        |
| Throughput            | 4557.54 transactions per second    | crowRedis        |
| Average response time | 0.0002 seconds                     | crowRedis        |
| Benchmark SET         | 1000 requests in 0.4349 seconds    | crowRedis        |
| Benchmark GET         | 1000 requests in 0.0271 seconds    | crowRedis        |
| Benchmark DEL         | 1000 requests in 0.0322 seconds    | crowRedis        |



| **Metric**            | **Value**                          | Database |
|-----------------------|------------------------------------| ---------|
| Total time taken      | 0.016948461532592773 seconds       | Redis  |
| Throughput            | 5900.24 transactions per second    | Redis  |
| Average response time | 0.0002 seconds                     | Redis  |
| Benchmark SET         | 1000 requests in 0.0280 seconds    | Redis  |
| Benchmark GET         | 1000 requests in 0.0320 seconds    | Redis  |
| Benchmark DEL         | 1000 requests in 0.0315 seconds    | Redis  |


# Using the Redis Client(client.py)


SET key value - Set the value of a key.  
Example:  
```redis
SET mykey Hello
```

GET key - Get the value associated with a key.  
Example:  
```redis
GET mykey
```

DEL key - Delete a key and its associated value.  
Example:  
```redis
DEL mykey
```

SAVE - Save data to a snapshot file.  
Example:  
```redis
SAVE
```

MULTI - Start a transaction block.  
Example:  
```redis
MULTI
```

EXEC - Execute the commands in a transaction.  
Example:  
```redis
MULTI
SET key1 value1
SET key2 value2
EXEC
```

DISCARD - Discard the commands in a transaction.  
Example:  
```redis
MULTI
SET key1 value1
DISCARD
```

LPUSH key value1 [value2 ...] - Insert one or more values at the beginning of a list.  
Example:  
```redis
LPUSH mylist item1
LPUSH mylist item2 item3
```

RPUSH key value1 [value2 ...] - Append one or more values to the end of a list.  
Example:  
```redis
RPUSH mylist item4
RPUSH mylist item5 item6
```

LPOP key - Remove and return the first element from a list.  
Example:  
```redis
LPOP mylist
```

RPOP key - Remove and return the last element from a list.  
Example:  
```redis
RPOP mylist
```

LRANGE key start stop - Get a range of elements from a list.  
Example:  
```redis
LRANGE mylist 0 2
```
```

Once you have the client running, you can interact with the crowRedis server. Here's how the client works:

To send a Redis command, enter it at the prompt and press Enter.
To exit the client, type 'exit' and press Enter.
Transaction Support
You can also work with transactions using the following commands:

To start a transaction, enter 'MULTI'.
Enter the transaction commands one by one.
To execute the transaction, enter 'EXEC'.
To discard the transaction, enter 'DISCARD'.
The client will send these commands to the server and display the responses.

Data Persistence
crowRedis supports data persistence through snapshot files and an append-only file (AOF). It automatically saves data to a snapshot file at regular intervals and recovers data from the AOF file upon server startup.

Transactions
crowRedis allows you to group multiple commands into a transaction using the MULTI command. You can add commands to the transaction and then use EXEC to execute them or DISCARD to cancel the transaction.

List Operations
crowRedis supports list operations, including LPUSH, RPUSH, LPOP, RPOP, and LRANGE, allowing you to manipulate lists stored as values in the data store.
