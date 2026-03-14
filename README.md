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

```bash
pip install -r requirements.txt
```

## Running

Start a cluster of nodes:
```bash
python node/server.py --node-id 1 --port 5001
python node/server.py --node-id 2 --port 5002
python node/server.py --node-id 3 --port 5003
```

Send messages via client:
```bash
python client/client.py --server localhost:5001
```

## Team Members

| Member   | Name    | Reg. No   |       Role            |
|----------|---------|-----------|-----------------------|
| Member 1 | Maheesha| IT24103477| Data Replication       |
| Member 2 | Sihan   | IT24103532| Fault Tolerance       |
| Member 3 | Shagee  | IT24103322| Time Synchronization  |
| Member 4 | Gunitha | IT24610787| Consensus & Agreement |

## License

This project is for academic purposes only.
