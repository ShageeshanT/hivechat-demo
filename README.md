# HiveChat: Resilient Distributed Messaging System

![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![gRPC](https://img.shields.io/badge/gRPC-Framework-green?style=for-the-badge&logo=grpc)
![Protocol Buffers](https://img.shields.io/badge/Protobuf-Serialization-orange?style=for-the-badge)
![License](https://img.shields.io/badge/License-Academic-lightgrey?style=for-the-badge)

## Executive Summary

HiveChat is a sophisticated, highly available distributed messaging architecture built entirely in Python. Designed to demonstrate advanced concepts in distributed systems engineering, it provides a robust infrastructure where clients can exchange messages seamlessly across a cluster of independent server nodes. 

By eliminating single points of failure, HiveChat ensures continuous operation, strict data consistency, causal message ordering, and automated state recovery in the event of network partitions or hardware failures.

## Architectural Design

The system relies on a decentralized cluster topology. Clients connect to the cluster and can seamlessly failover between nodes without interrupting the user experience. 

```text
    [Client Applications] 
          │      │      │
          ▼      ▼      ▼
   ┌──────────────────────────┐
   │    HiveChat Cluster      │
   │                          │
   │  [Node 1] -- [Node 2]    │
   │      |    \/    |        │
   │      |    /\    |        │
   │  [Node 3] -- [Node 4]    │
   └──────────────────────────┘
```

When a user submits a message, the receiving node processes it through four critical internal subsystems before marking it as delivered. This pipeline guarantees fault tolerance, persistence, spatial synchronization, and temporal ordering.

## Core Subsystems and Research Areas

HiveChat integrates four fundamental distributed computing modules, each managed by a dedicated mechanism:

### 1. Fault Tolerance & Recovery (Sihan - IT24103532)
Ensures system liveness and data durability despite unexpected node crashes.
- **Persistent Storage Engine**: Messages are persisted to disk using SQLite/JSON with unique idempotency keys to prevent duplication during network retries.
- **Failure Detection**: Implements a heartbeat mechanism that proactively probes peers. A threshold of consecutive missed beats flags a node as dead.
- **Automated Catch-Up**: Upon rejoining the network, a recovering node automatically queries healthy peers to fetch and replay missed history.
- **Pending Queues**: Disconnected peers do not lose messages; the active cluster queues outgoing replicas and pushes them immediately upon the peer's return.

### 2. Data Replication (Maheesha - IT24103477)
Guarantees data availability and consistency across the physical cluster.
- **Quorum-Based Replication**: Read and write operations require acknowledgments from a calculated quorum to ensure strict consistency.
- **Conflict Resolution**: Employs vector clocks and causal tracking to deduplicate and resolve conflicting state writes across distributed geographic boundaries.

### 3. Time Synchronization (Shagee - IT24103322)
Maintains temporal order in an environment lacking a shared global clock.
- **Clock Synchronization**: Implements an NTP-style offset calculation algorithm to keep node clocks synchronized against an authoritative reference.
- **Causal Ordering**: Utilizes Lamport timestamps to enforce chronological message reordering on the receiving end, preserving conversational continuity.

### 4. Raft Consensus (Gunitha - IT24610787)
Establishes distributed agreement without human intervention.
- **Leader Election**: Automated, randomized election timeouts ensure a single authoritative leader is chosen flawlessly.
- **Log Replication**: The elected leader coordinates strict log replication across followers, ensuring the global state machine is deterministic and synchronized.

## Technology Stack

- **Core Runtime**: Python 3.10+
- **RPC Framework**: gRPC (for high-performance, strongly-typed node and client communication)
- **Serialization**: Protocol Buffers (protobuf)
- **Persistence**: Hybrid In-Memory & SQLite storage

## Project Organization

```text
hivechat-demo/
├── proto/              # Protocol Buffer definitions and generated gRPC stubs
├── node/               # Server-side cluster modules
│   ├── server.py       # Cluster initialization and gRPC servicers
│   ├── consensus.py    # Raft leader election and log state machine
│   ├── replication.py  # Quorum read/write logic
│   ├── time_sync.py    # Logical and physical clock synchronization
│   └── fault.py        # Failure detection and node recovery logic
├── client/             # User-facing application
│   └── client.py       # gRPC client with built-in failover capabilities
├── tests/              # Automated integration and unit tests
└── requirements.txt    # Python dependencies
```

## Quick Start Guide

### Prerequisites
- Python 3.10 or higher
- `pip` package manager

### Installation

1. Clone the repository and navigate to the project directory.

2. Create and activate a Python virtual environment:
   
   **Windows:**
   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```
   **macOS/Linux:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install necessary dependencies:

```bash
pip install -r requirements.txt
```

4. (Optional) Regenerate gRPC stubs if modifications are made to `proto/hivechat.proto`:

```bash
python -m grpc_tools.protoc -I proto --python_out=proto --grpc_python_out=proto proto/hivechat.proto
```

## Running the Distributed Cluster

To observe the distributed mechanics in action, you can simulate a three-node cluster locally using different terminal windows. 

**Terminal 1: Bootstrap Node 1**
```bash
python node/server.py --node-id 1 --port 5001 --demo
```

**Terminal 2: Attach Node 2**
```bash
python node/server.py --node-id 2 --port 5002 --peers localhost:5001 --demo
```

**Terminal 3: Attach Node 3**
```bash
python node/server.py --node-id 3 --port 5003 --peers localhost:5001,localhost:5002 --demo
```

### Interactive Node Commands
The `--demo` flag enables a rich interactive shell on each server. Available commands:
- `elect`: Force a Raft leader election on the current node.
- `status`: Display current Raft state, leader identity, log length, and time-sync offsets.
- `metrics`: Output detailed replication health and node performance telemetry.
- `peers`: Visualize the heartbeat status of surrounding nodes in the cluster.
- `messages`: Export the local persistent storage table to the console.

## Interacting with the Network

The client application includes sophisticated failover routing. If the primary node crashes, the client automatically re-routes the operation to the next healthy node in the list.

**Launch the Client:**
```bash
python client/client.py --user Alice --servers localhost:5001,localhost:5002,localhost:5003
```

**Client Commands:**
- `@Bob Hello there!` : Sends a message to user 'Bob'.
- `/inbox` : Polls the cluster for unread messages.
- `/servers` : Displays the current failover state and active connection.

### Simulating a Node Failure
1. Start the cluster and connect a client as shown above.
2. Abruptly terminate Terminal 1 (`Ctrl+C`).
3. Have the client send a new message.
4. Watch the client terminal instantly detect the failure and reroute the payload to `localhost:5002` transparently.

## Licensing

This repository and its contents have been developed for academic and conceptual demonstration purposes. Strict academic integrity rules apply regarding the reproduction of these specialized algorithms.
