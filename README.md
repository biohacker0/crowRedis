# crowRedis dataRepilication-1 branch
I built my own small simple memory datastore like redis to learn how it works internally and how databases are built , it has set,get, transactions (multi/exec ),persistence(snapshots, AOF) , concurrency support for all things and concurrent transactions , and lots of data replication if you run the servers in master/worker mode

# [Blog](https://corvus-ikshana.hashnode.dev/crowredis-data-replication-delving-into-distributed-systems)

This brach is specificallu made to test my data replication feature for crowRedis.

# Distributed Key-Value Store with Data Replication

This Python script implements a distributed key-value store with support for data replication. It can be run in both master and worker modes to enable replication between servers. Here's an overview of how data replication works and how to run it:

## Data Replication

### Replication Overview

Data replication in this key-value store is designed to ensure that multiple servers stay synchronized with each other. This can be useful for building distributed and fault-tolerant systems.

#### Components

- **Master Server**: The server that holds the authoritative data and logs all changes.
- **Worker Servers**: Servers that connect to the master server to replicate data and stay up-to-date.

### How Replication Works

1. The master server logs executed commands to an AOF (Append-Only File) and adds them to a log queue.
2. Worker servers connect to the master and continuously fetch commands from the log queue.
3. The worker servers apply these commands to their local data store, keeping it in sync with the master.

### How to run the project:
1 : clone the project twice and name one folder as instace-1 and other instance -2 , it will be easisy this way to test the feature.
2 : run crowRedis.py in master mode in one instnace and other in worker mode and change the port no of woker in the instance-2 folder

## Running in Master/Worker Mode

To run this key-value store in master/worker mode and enable data replication, follow these steps:

### Master Server

1. Set up the master server with the following options:
   - Host: Specify the IP address or hostname for the master server.
   - Port: Choose a port number for the master server.
   - Is Master: Set to "yes."

2. Start the master server using the following command:
   ```bash
   python crowRedis.py
   

Worker Server
Set up the worker server with the following options:

- Master Address: Provide the address of the master server in the format "hostname:port."
- Host: Specify the IP address or hostname for the worker server.
- Port: Choose a port number for the worker server.
- Is Master: Set to "no."

Start the worker server using the following command:
   ```bash
   python crowRedis.py

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Monitoring Data Replication
You can monitor the data replication process by checking the log file named server.log. It records information about connected workers, sent and received data, and errors.
To ensure proper network configuration, make sure the master and worker servers can communicate with each other.
