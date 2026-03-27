# HiveChat

A fault-tolerant distributed messaging system built with Python.

## Overview

HiveChat is a distributed messaging system where clients send messages to each other through a cluster of distributed servers. The system supports real-time message delivery, storage, and retrieval while ensuring high availability, fault tolerance, and consistency.

## Architecture

```
Clients  ──►  Server Cluster (3-5 nodes)  ──►  Clients
                    │
           ┌────────┼────────┐────────┐
        Node 1   Node 2   Node 3   Node N
        (Leader) (Follower)(Follower)
```

## Core Components

| Component | Description |
|-----------|-------------|
| **Fault Tolerance** | Failure detection, message redundancy, automatic failover, and node recovery |
| **Data Replication** | Quorum-based replication with consistency guarantees and deduplication |
| **Time Synchronization** | Clock sync protocol, Lamport clocks, and message reordering |
| **Consensus (Raft)** | Leader election, log replication, and distributed agreement |

## Tech Stack

- **Language:** Python 3.10+
- **Communication:** gRPC
- **Storage:** In-memory / SQLite
- **Serialization:** Protocol Buffers

## Project Structure

```
hivechat/
├── proto/              # gRPC protobuf definitions
├── node/               # Server node modules
│   ├── server.py       # Main server entry point
│   ├── consensus.py    # Raft consensus logic
│   ├── replication.py  # Data replication
│   ├── time_sync.py    # Time synchronization
│   └── fault.py        # Fault tolerance & recovery
├── client/             # Client application
│   └── client.py
├── tests/              # Test scripts
├── requirements.txt
└── README.md
```

## Setup

1. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Generate gRPC Stubs (optional):**
   *(The project already includes generated stubs in `proto/`, but you can regenerate them if you change `hivechat.proto`)*
   ```bash
   python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/hivechat.proto
   ```

## Running a Fault-Tolerant Cluster

To test liveness, replication, and failover, start a cluster of 3 nodes:

1. **Start Node 1 (port 5001):**
   ```bash
   # Demo mode allows interactive testing of metrics and messages
   python node/server.py --node-id 1 --port 5001 --demo
   ```

2. **Start Node 2 (port 5002 - connected to Node 1):**
   ```bash
   python node/server.py --node-id 2 --port 5002 --peers localhost:5001 --demo
   ```

3. **Start Node 3 (port 5003 - connected to Node 1 and 2):**
   ```bash
   python node/server.py --node-id 3 --port 5003 --peers localhost:5001,localhost:5002 --demo
   ```

## Client usage with Failover

The client automatically tries the next server if the primary one is down.

```bash
# Provide all server addresses for failover support
python client/client.py --user Sihan --servers localhost:5001,localhost:5002,localhost:5003
```

- Try killing a node (e.g., Node 1) and see the client automatically deliver through Node 2.
- Type `/inbox` to see messages received for your user.
- Type `metrics` in a server window to see the replication status and storage overhead.

## Fault Tolerance (Member 2 Features)

As the Fault Tolerance member (Sihan, IT24103532), I've implemented:
- **Persistent Message Store:** Messages are saved in JSON files with `message_id` deduplication.
- **Failure Detection:** Standard threshold-based heartbeat detector (marks a node dead if 3 pings fail).
- **Automatic Recovery:** When a node rejoins, it fetches missing history from its peers.
- **Pending Replication Queue:** Messages that fail to replicate while a peer is down are queued and automatically pushed once the peer recovers.
- **Redundancy Metrics:** Tracks per-peer replication success rates and storage overhead.

## License

This project is for academic purposes only.
