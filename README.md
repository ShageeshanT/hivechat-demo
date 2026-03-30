# HiveChat — Fault-Tolerant Distributed Messaging System

![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue?style=for-the-badge&logo=python)
![gRPC](https://img.shields.io/badge/gRPC-Framework-green?style=for-the-badge&logo=grpc)
![Protocol Buffers](https://img.shields.io/badge/Protobuf-Serialization-orange?style=for-the-badge)
![SQLite](https://img.shields.io/badge/SQLite-Persistence-yellow?style=for-the-badge&logo=sqlite)
![Raft](https://img.shields.io/badge/Raft-Consensus-purple?style=for-the-badge)
![License](https://img.shields.io/badge/License-Academic-lightgrey?style=for-the-badge)

HiveChat is a distributed messaging system built in Python that demonstrates four core distributed computing principles working together: **Fault Tolerance**, **Data Replication**, **Time Synchronization**, and **Raft Consensus**. Any node in a cluster can fail and the system continues operating — messages are never lost.

---

## Team

| Member | ID | Module |
|--------|----|--------|
| Sihan | IT24103532 | Fault Tolerance & Recovery (`fault.py`) |
| Maheesha | IT24103477 | Data Replication & Vector Clocks (`replication.py`) |
| Shagee | IT24103322 | Time Synchronization & Causal Ordering (`time_sync.py`) |
| Gunitha | IT24610787 | Raft Consensus & Leader Election (`consensus.py`) |

---

## Architecture

Each node in the cluster runs four integrated modules. Every message flows through all of them:

```
┌─────────────────────────────────────────────────────────────────┐
│                     HiveChat Cluster                            │
│                                                                 │
│   Client ──gRPC──► Node 1 (any port)                           │
│                        │                                        │
│          ┌─────────────▼──────────────────────┐                │
│          │  1. gRPC Servicers (server.py)      │                │
│          │     SendMessage / GetMessages       │                │
│          │     Heartbeat / Replicate           │                │
│          │     GetTime (TimeSyncService)       │                │
│          └──┬──────────────┬──────────────┬───┘                │
│             │              │              │                     │
│    ┌────────▼───┐  ┌───────▼────┐  ┌─────▼──────┐             │
│    │  Fault     │  │Replication │  │Time Sync   │             │
│    │Tolerance   │  │ Manager    │  │ Module     │             │
│    │(SQLite)    │  │(VectorClk) │  │(Lamport,   │             │
│    │fault.py    │  │replication │  │ NTP, Causal│             │
│    │            │  │    .py     │  │ Reorder)   │             │
│    └────────────┘  └────────────┘  └────────────┘             │
│             │                                                   │
│    ┌────────▼───────────────────────────┐                      │
│    │  Raft Consensus (consensus.py)     │                      │
│    │  Leader Election + Log Replication │                      │
│    └──────────────────────────────────-─┘                      │
│                                                                 │
│   Node 1 ◄──── gRPC RPCs ────► Node 2 ◄──── gRPC ────► Node 3 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Message Flow (What Happens When You Send a Message)

```
Client sends "Hello" to Bob
        │
        ▼
[1] gRPC SendMessage RPC → any live node
        │
        ▼
[2] FaultToleranceManager.handle_client_message()
        │  ├─ Saves message to SQLite (durable, survives restarts)
        │  ├─ Advances Lamport clock + vector clock (via ReplicationManager)
        │  ├─ Marks message in MessageReorderer for causal delivery
        │  └─ Replicates to ALL live peers via gRPC Replicate RPC
        │              │
        │              ▼
        │    [3] Each peer: FaultToleranceManager.handle_replica_message()
        │              │  ├─ Saves replica to peer's SQLite
        │              │  └─ ReplicationManager.receive_replica()
        │                       └─ Merges vector clock
        │                       └─ MessageReorderer: causal ordering check
        ▼
[4] Returns StatusResponse (message_id, status=stored_and_replicated)
        │
        ▼
[5] Web UI polls /api/all-messages → queries ALL nodes → deduplicates → displays
```

If a peer is **offline** when step 3 runs, the message is added to a **SQLite-backed pending queue**. When that peer comes back online, the queue is automatically drained via the heartbeat recovery path.

---

## Module Details

### 1. Fault Tolerance (`node/fault.py`) — Sihan

The backbone of the system. Every message passes through this module.

**Components:**

| Class | Role |
|-------|------|
| `PersistentMessageStore` | SQLite-backed store with WAL mode. `INSERT OR IGNORE` for idempotent dedup by `message_id`. |
| `FailureDetector` | Parallel heartbeat loop per peer. Peers start **optimistically alive**. Declares a peer dead after `missed_threshold` (default 3) consecutive missed heartbeats. |
| `PendingReplicationQueue` | SQLite queue of `(peer, message_id)` pairs. Drains automatically when a dead peer recovers. |
| `FaultToleranceManager` | Orchestrates all three. Handles client writes, replica writes, peer recovery, and the retry background thread. |

**Fault recovery timeline:**
```
Node goes down  →  FailureDetector marks it DEAD  →  new messages queued
Node comes back →  FailureDetector detects recovery
                →  recover_from_peers() pulls missed messages from peers
                →  PendingReplicationQueue drains queued replicas to peer
```

**Key configuration:**
- `replication_factor` = `len(peers) + 1` — every node holds a full copy of every message
- `heartbeat_interval` = 3 seconds
- `missed_threshold` = 3 missed beats → peer declared dead

---

### 2. Data Replication (`node/replication.py`) — Maheesha

Maintains causal consistency and quorum guarantees across the cluster.

**Components:**

| Class | Role |
|-------|------|
| `VectorClock` | Per-node logical clock. `tick()` before sending, `update(received_vc)` on receive. `happened_before()` and `concurrent()` helpers for causal reasoning. |
| `MessageStore` | Thread-safe in-memory store tracking `pending` / `committed` status per message. |
| `ReplicationManager` | Quorum write (W), quorum read (R), anti-entropy sync, and Raft `apply_committed_entry()` integration. |

**Quorum model (N=cluster size, W=ceil(N/2)+1, R=ceil(N/2)+1):**
```
Write path:  save local → forward to W-1 peers → if acks ≥ W → mark committed
Read path:   read local + R-1 peers → merge by dedup → sort by vector clock
W + R > N   → reads always see the latest committed write
```

**Integration with live message path:**
Every `SendMessage` gRPC call not only persists to SQLite (FaultToleranceManager) but also:
1. Ticks the vector clock via `ReplicationManager.vector_clock.tick()`
2. Ticks the Lamport clock via `TimeSyncer.lamport.tick()`
3. Saves to the in-memory `MessageStore`

Every `Replicate` gRPC call also calls `ReplicationManager.receive_replica()` which merges the incoming vector clock and passes the message through `MessageReorderer` for causal delivery ordering.

---

### 3. Time Synchronization (`node/time_sync.py`, `node/time_sync_service.py`) — Shagee

Keeps all nodes' clocks aligned and enforces causal message order.

**Components:**

| Class | Role |
|-------|------|
| `LamportClock` | Scalar logical clock. `tick()` before every send, `update(received)` on receive. Thread-safe. |
| `TimeSyncer` | Cristian's Algorithm (NTP-style). Computes `offset = server_time − (t_send + RTT/2)`. Keeps last 8 samples and uses the **median** to suppress outliers. Syncs every 5 seconds in a background thread. |
| `MessageReorderer` | Buffers messages that arrive before their causal predecessors. Delivers when the vector clock condition is met. Force-delivers after 10 s to prevent starvation. |
| `TimeSyncServicer` | gRPC service (`GetTime` RPC). Runs on the same port as the main node server. Peers call this to estimate their clock offset. |
| `SyncConfig` | Loads all tunable parameters from `config/time_sync.json`. Falls back to built-in defaults. |

**Clock offset algorithm:**
```python
t_send   = time.time()
# ... gRPC GetTime RPC to leader ...
t_recv   = time.time()
rtt      = t_recv - t_send
offset   = server_time - (t_send + rtt / 2)

# Keep last 8 samples, use MEDIAN to suppress RTT outliers
self._samples.append(offset)
self.offset = statistics.median(self._samples)

# Applied to every outgoing message timestamp:
message["timestamp"] = time.time() + self.offset
```

**Causal delivery condition (MessageReorderer):**
```
Message from node S with vector_clock V is deliverable when:
  V[S] == delivered[S] + 1     (next expected event from sender S)
  V[N] <= delivered[N]          for all other nodes N
```

**After a Raft leader election**, the `TimeSyncer` is automatically pointed at the new leader's address so clock offsets always reflect the authoritative node.

---

### 4. Raft Consensus (`node/consensus.py`) — Gunitha

Establishes a single authoritative leader for the cluster without manual intervention.

**States:**
```
FOLLOWER ──[election timeout]──► CANDIDATE ──[majority votes]──► LEADER
    ▲                                  │                             │
    └───────[higher term seen]─────────┘◄────[higher term seen]─────┘
    └───────[valid AppendEntries received]────────────────────────────┘
```

**Key properties:**
- **Term-based elections**: Each election increments the term. A node only votes once per term.
- **Log completeness check**: A candidate is only voted for if its log is at least as up-to-date as the voter's (Raft §5.4.1).
- **Commit rule**: An entry is committed only when `⌊N/2⌋ + 1` nodes have acknowledged it, and only for entries from the current term (Raft §5.4.2).
- **State machine**: After majority commit, `replication.apply_committed_entry(entry)` is called so the message appears in the ReplicationManager's store.

**Interface contract with Replication module:**
```python
# Replication queries consensus:
leader_id = consensus.get_leader()   # int node_id, -1 if unknown
am_leader = consensus.is_leader()    # True if this node is the current leader

# Consensus calls replication after committing:
replication.apply_committed_entry({"term": int, "message": str})
```

---

## Project Structure

```
hivechat-demo/
├── proto/
│   ├── hivechat.proto          # Service definitions (MessagingService, FaultService, TimeSyncService)
│   ├── hivechat_pb2.py         # Generated protobuf message classes
│   └── hivechat_pb2_grpc.py    # Generated gRPC stubs
│
├── node/
│   ├── server.py               # HiveChatNode: wires all 4 modules + gRPC servicers
│   ├── fault.py                # PersistentMessageStore, FailureDetector, PendingReplicationQueue, FaultToleranceManager
│   ├── replication.py          # VectorClock, MessageStore, ReplicationManager
│   ├── time_sync.py            # LamportClock, TimeSyncer, MessageReorderer
│   ├── time_sync_service.py    # TimeSyncServicer gRPC handler + sync_once() helper
│   ├── sync_config.py          # SyncConfig: load time sync parameters from JSON
│   ├── consensus.py            # RaftNode: leader election + log replication
│   └── __init__.py             # Exports all public classes from all modules
│
├── client/
│   └── client.py               # HiveChatClient: gRPC transport with automatic failover
│
├── templates/
│   └── index.html              # Web UI (Telegram-style dark theme, real-time polling)
│
├── tests/
│   ├── test_fault.py           # FaultToleranceManager, FailureDetector, PendingReplicationQueue
│   ├── test_replication.py     # MessageStore, VectorClock, ReplicationManager, Raft integration
│   ├── test_consensus.py       # RaftNode elections, log replication, commit rules
│   ├── test_time_sync.py       # LamportClock, TimeSyncer, MessageReorderer
│   └── test_integration.py     # End-to-end cross-module integration tests
│
├── config/
│   └── time_sync.json          # Optional: override sync_interval, buffer_timeout, etc.
│
├── web_client.py               # Flask app: REST API proxy between browser and gRPC cluster
├── generate_report.py          # Generates hivechat_report.html (project status report)
└── requirements.txt            # grpcio, protobuf, flask, pytest
```

---

## gRPC Services

Three gRPC services run on every node, all sharing the same port:

```protobuf
// Client → Node
service MessagingService {
  rpc SendMessage (SendMessageRequest)  returns (StatusResponse);
  rpc GetMessages (GetMessagesRequest)  returns (GetMessagesResponse);
}

// Node → Node (fault tolerance)
service FaultService {
  rpc Heartbeat  (HeartbeatRequest)  returns (HeartbeatResponse);
  rpc Replicate  (ReplicateRequest)  returns (StatusResponse);
}

// Node → Node (time synchronization)
service TimeSyncService {
  rpc GetTime    (TimeSyncRequest)   returns (TimeSyncResponse);
}
```

---

## Quick Start

### Prerequisites
- Python 3.10+
- `pip`

### Installation

```bash
# Clone and enter the project
git clone https://github.com/ShageeshanT/hivechat-demo.git
cd hivechat-demo

# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate
# macOS/Linux
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running a 3-Node Cluster

Open **4 separate terminal windows**, all in the project root with the venv activated:

**Terminal 1 — Node 1 (standalone first)**
```bash
python node/server.py --node-id 1 --port 5001
```

**Terminal 2 — Node 2**
```bash
python node/server.py --node-id 2 --port 5002 --peers localhost:5001
```

**Terminal 3 — Node 3**
```bash
python node/server.py --node-id 3 --port 5003 --peers localhost:5001,localhost:5002
```

**Terminal 4 — Web UI**
```bash
python web_client.py
```

Open your browser at **http://localhost:8000**, enter a username, and start chatting.

### Adding More Nodes Dynamically

Any additional node automatically recovers all existing messages from peers on startup:

```bash
python node/server.py --node-id 4 --port 5004 --peers localhost:5001,localhost:5002,localhost:5003
```

Output:
```
[Recovery] Fetched 5 message(s) from peer localhost:5001
[Recovery] Fetched 5 message(s) from peer localhost:5002
[Recovery] Fetched 5 message(s) from peer localhost:5003
[HiveChat] Node 4 is FULLY READY.
```

---

## Interactive Demo Commands

The `--demo` flag opens an interactive shell on the server process:

```bash
python node/server.py --node-id 1 --port 5001 --demo
```

| Command | Output |
|---------|--------|
| `status` | Raft state, current leader, term, time-sync offset, Lamport clock |
| `elect` | Force a Raft leader election on this node |
| `peers` | Live / dead status of each peer with last-seen latency |
| `metrics` | Replication success rate, pending queue depth, storage bytes |
| `messages` | All messages stored in this node's SQLite database |

---

## Fault Tolerance Demo

### Kill any node — messages survive

```bash
# Send some messages with 3 nodes running
# Then kill Node 1 (Ctrl+C in Terminal 1)
# Node 2 and 3 still hold all messages (full replication)
# Web UI automatically queries surviving nodes
```

### Node recovery — missed messages restored

```bash
# Kill Node 2, send more messages with Node 1 and 3
# Restart Node 2:
python node/server.py --node-id 2 --port 5002 --peers localhost:5001

# Node 2 automatically fetches all missed messages from peers on startup
# The pending replication queue on Node 1 and 3 also drains to Node 2
```

### CLI Client (optional)

```bash
python client/client.py --user Alice --servers localhost:5001,localhost:5002,localhost:5003
```

| Command | Action |
|---------|--------|
| `@Bob Hello!` | Send message to Bob |
| `/inbox` | Fetch all your received messages |
| `/servers` | Show current failover state |

---

## Running Tests

```bash
python -m pytest tests/ -v
```

Expected: **174 tests pass** across all four modules plus integration tests.

```bash
# Run a specific module's tests
python -m pytest tests/test_fault.py -v
python -m pytest tests/test_replication.py -v
python -m pytest tests/test_consensus.py -v
python -m pytest tests/test_time_sync.py -v
```

---

## Regenerating Proto Stubs

If you modify `proto/hivechat.proto`:

```bash
python -m grpc_tools.protoc \
  -I proto \
  --python_out=proto \
  --grpc_python_out=proto \
  proto/hivechat.proto
```

---

## Configuration

### Time Synchronization (`config/time_sync.json`)

Create this file to override defaults without editing source code:

```json
{
  "sync_interval": 5.0,
  "sample_count": 8,
  "max_offset_ms": 500,
  "buffer_timeout": 10.0
}
```

| Key | Default | Description |
|-----|---------|-------------|
| `sync_interval` | `5.0` s | How often TimeSyncer polls the leader for offset |
| `sample_count` | `8` | Number of offset samples for median filter |
| `max_offset_ms` | `500` ms | Log a warning if offset exceeds this |
| `buffer_timeout` | `10.0` s | Force-deliver causally buffered messages after this timeout |

---

## Consistency Guarantees

| Property | Guarantee |
|----------|-----------|
| **Durability** | Every message is written to SQLite on at least one node before acknowledgement |
| **Full replication** | Every node holds a complete copy — any single node failure is survivable |
| **Causal ordering** | Vector clocks ensure messages are delivered in causal order |
| **Temporal ordering** | Lamport clocks + NTP-style offset ensure consistent timestamps across nodes |
| **No data loss on restart** | SQLite WAL mode + startup peer recovery restores all missed messages |
| **No duplicates** | `INSERT OR IGNORE` by `message_id` (UUID) prevents double-storing on retries |

---

## License

Developed for academic demonstration purposes as a Distributed Systems course project. Strict academic integrity rules apply.
