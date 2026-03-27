"""
HiveChat – Main Server Node
============================
Entry point for starting a distributed messaging node.
Integrates ALL FOUR distributed-system modules:
  • Fault Tolerance        (Member 2 – Sihan)
  • Data Replication       (Member 3 – Maheesha)
  • Time Synchronization   (Member 4 – Shagee)
  • Raft Consensus         (Member 5 – Gunitha)

Run:
    python node/server.py --node-id 1 --port 5001
    python node/server.py --node-id 2 --port 5002 --peers localhost:5001
    python node/server.py --node-id 3 --port 5003 --peers localhost:5001,localhost:5002
"""

import argparse
import sys
import time
from concurrent import futures
from pathlib import Path

import grpc

# ── path setup (works whether run from project root or node/) ─────────────────
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
PROTO_DIR = PROJECT_ROOT / "proto"
if str(PROTO_DIR) not in sys.path:
    sys.path.append(str(PROTO_DIR))

# ── generated gRPC stubs ──────────────────────────────────────────────────────
from proto import hivechat_pb2, hivechat_pb2_grpc

# ── distributed system modules ────────────────────────────────────────────────
from node.fault import FaultToleranceManager
from node.replication import ReplicationManager
from node.time_sync import TimeSyncer, MessageReorderer
from node.consensus import RaftNode

# Heartbeat RPC timeout (seconds) – short so failure detection stays responsive
HEARTBEAT_TIMEOUT = 2.0
# Replication RPC timeout
REPLICATE_TIMEOUT = 5.0
# Recovery fetch timeout
FETCH_TIMEOUT = 10.0


# ─────────────────────────────────────────────────────────────────────────────
# gRPC Servicers  (server-side RPC implementations)
# ─────────────────────────────────────────────────────────────────────────────

class MessagingServicer(hivechat_pb2_grpc.MessagingServiceServicer):
    """
    Handles client → node RPCs:
      • SendMessage – client submits a new chat message
      • GetMessages – recovering node pulls all stored messages
    """

    def __init__(self, node: "HiveChatNode"):
        self._node = node

    def SendMessage(self, request, context):
        msg_proto = request.message
        message = {
            "message_id":  msg_proto.message_id,
            "sender":      msg_proto.sender,
            "receiver":    msg_proto.receiver,
            "content":     msg_proto.content,
            "timestamp":   msg_proto.timestamp,
            "origin_node": msg_proto.origin_node,
        }
        result = self._node.fault_manager.handle_client_message(message)
        return hivechat_pb2.StatusResponse(
            success=True,
            status=result["status"],
            message_id=result["message_id"],
            node_id=result["node_id"],
        )

    def GetMessages(self, request, context):
        """Return all locally stored messages (used by recovering peers)."""
        all_msgs = self._node.fault_manager.export_messages()
        proto_msgs = [
            hivechat_pb2.ChatMessage(
                message_id  = m["message_id"],
                sender      = m["sender"],
                receiver    = m["receiver"],
                content     = m["content"],
                timestamp   = float(m["timestamp"]),
                origin_node = m["origin_node"],
            )
            for m in all_msgs
        ]
        return hivechat_pb2.GetMessagesResponse(messages=proto_msgs)


class FaultServicer(hivechat_pb2_grpc.FaultServiceServicer):
    """
    Handles node → node RPCs for fault tolerance:
      • Heartbeat – liveness probe
      • Replicate – peer pushing a message replica
    """

    def __init__(self, node: "HiveChatNode"):
        self._node = node

    def Heartbeat(self, request, context):
        """Always respond alive=True while this node is running."""
        return hivechat_pb2.HeartbeatResponse(
            alive=True,
            node_id=f"node{self._node.node_id}",
        )

    def Replicate(self, request, context):
        msg_proto = request.message
        message = {
            "message_id":  msg_proto.message_id,
            "sender":      msg_proto.sender,
            "receiver":    msg_proto.receiver,
            "content":     msg_proto.content,
            "timestamp":   msg_proto.timestamp,
            "origin_node": msg_proto.origin_node,
        }
        result = self._node.fault_manager.handle_replica_message(message)
        return hivechat_pb2.StatusResponse(
            success=True,
            status=result["status"],
            message_id=result["message_id"],
            node_id=result["node_id"],
        )


# ─────────────────────────────────────────────────────────────────────────────
# HiveChatNode – wires all four modules together
# ─────────────────────────────────────────────────────────────────────────────

class HiveChatNode:
    """
    Main node wrapper for HiveChat.

    Wires all four distributed-system modules together:
      1. TimeSyncer           – NTP-style clock offset + Lamport clock
      2. MessageReorderer     – causal delivery buffer (used by ReplicationManager)
      3. ReplicationManager   – quorum-based message store + vector clocks
      4. RaftNode             – leader election + log replication → feeds ReplicationManager
      5. FaultToleranceManager– heartbeat monitoring + persistent SQLite store + retry queue
    """

    def __init__(self, node_id: int, port: int, peers: list):
        self.node_id = node_id
        self.port    = port
        self.address = f"localhost:{port}"
        self.peers   = peers

        # ── Persistent storage path ───────────────────────────────────────
        data_dir = PROJECT_ROOT / "node" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        store_path = str(data_dir / f"node{self.node_id}_messages.db")

        # ── 1. Time Synchronization ───────────────────────────────────────
        # Starts with no reference; updated when Raft elects a leader.
        print(f"[HiveChat] Initializing Time Synchronization module …")
        self.time_syncer = TimeSyncer(
            node_id=self.node_id,
            reference_addr=None,   # Raft will set this after election
        )
        self.reorderer = MessageReorderer()

        # ── 2. Data Replication ───────────────────────────────────────────
        # Uses TimeSyncer for offset-corrected timestamps and MessageReorderer
        # for causal delivery ordering.
        print(f"[HiveChat] Initializing Data Replication module …")
        cluster_size = len(self.peers) + 1
        self.replication_manager = ReplicationManager(
            node_id=self.node_id,
            peers=self.peers,
            all_node_ids=list(range(1, cluster_size + 1)),
            quorum_w=min(2, cluster_size),
            quorum_r=min(2, cluster_size),
            consensus=None,          # Raft wired in below
            time_syncer=self.time_syncer,
            reorderer=self.reorderer,
        )

        # ── 3. Raft Consensus ─────────────────────────────────────────────
        # Peers are wired after all nodes are created via set_raft_peers().
        # Replication module is registered so committed entries are applied.
        print(f"[HiveChat] Initializing Raft Consensus module …")
        self.raft = RaftNode(
            node_id=self.node_id,
            peers=[],                # populated via set_raft_peers()
            replication=self.replication_manager,
        )
        # Back-reference: ReplicationManager now knows its consensus module
        self.replication_manager.consensus = self.raft

        # ── 4. Fault Tolerance Manager ────────────────────────────────────
        print(f"[HiveChat] Initializing Fault Tolerance module …")
        self.fault_manager = FaultToleranceManager(
            node_id=f"node{self.node_id}",
            peers=self.peers,
            heartbeat_fn=self._heartbeat_peer,
            replicate_fn=self._replicate_to_peer,
            fetch_messages_fn=self._fetch_messages_from_peer,
            store_path=store_path,
            replication_factor=2,
            heartbeat_interval=3.0,
            missed_threshold=3,        # 3 missed beats → mark DEAD
        )

        # ── gRPC server handle ────────────────────────────────────────────
        self._grpc_server = None

    # ── gRPC transport helpers ────────────────────────────────────────────

    def _heartbeat_peer(self, peer_address: str) -> bool:
        """Open a short-lived gRPC channel and call the Heartbeat RPC."""
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub    = hivechat_pb2_grpc.FaultServiceStub(channel)
                request = hivechat_pb2.HeartbeatRequest(
                    sender_node_id=f"node{self.node_id}"
                )
                resp = stub.Heartbeat(request, timeout=HEARTBEAT_TIMEOUT)
                return resp.alive
        except Exception:
            return False

    def _replicate_to_peer(self, peer_address: str, message: dict) -> bool:
        """Open a gRPC channel and call the Replicate RPC."""
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub    = hivechat_pb2_grpc.FaultServiceStub(channel)
                request = hivechat_pb2.ReplicateRequest(
                    source_node_id=f"node{self.node_id}",
                    message=hivechat_pb2.ChatMessage(
                        message_id  = message["message_id"],
                        sender      = message["sender"],
                        receiver    = message["receiver"],
                        content     = message["content"],
                        timestamp   = float(message["timestamp"]),
                        origin_node = message["origin_node"],
                    ),
                )
                resp = stub.Replicate(request, timeout=REPLICATE_TIMEOUT)
                print(
                    f"[Replication] node{self.node_id} → {peer_address} "
                    f"(id={message['message_id'][:8]}…) status={resp.status}"
                )
                return resp.success
        except Exception as exc:
            print(f"[Replication] FAILED → {peer_address}: {exc}")
            return False

    def _fetch_messages_from_peer(self, peer_address: str) -> list:
        """Call GetMessages RPC and convert proto → dicts."""
        try:
            with grpc.insecure_channel(peer_address) as channel:
                stub    = hivechat_pb2_grpc.MessagingServiceStub(channel)
                request = hivechat_pb2.GetMessagesRequest(
                    node_id=f"node{self.node_id}"
                )
                resp = stub.GetMessages(request, timeout=FETCH_TIMEOUT)
                messages = [
                    {
                        "message_id":  m.message_id,
                        "sender":      m.sender,
                        "receiver":    m.receiver,
                        "content":     m.content,
                        "timestamp":   m.timestamp,
                        "origin_node": m.origin_node,
                    }
                    for m in resp.messages
                ]
                print(
                    f"[Recovery] Fetched {len(messages)} message(s) "
                    f"from peer {peer_address}"
                )
                return messages
        except Exception as exc:
            print(f"[Recovery] FAILED fetching from {peer_address}: {exc}")
            return []

    # ── Raft peer wiring ──────────────────────────────────────────────────

    def set_raft_peers(self, raft_peers: list) -> None:
        """
        Wire other RaftNode objects into this node's Raft instance.
        Call AFTER all nodes have been constructed (multi-node tests /
        cluster bootstrapper).
        """
        self.raft.set_peers(raft_peers)

    def _update_time_sync_reference(self) -> None:
        """
        After a leader election, point TimeSyncer at the leader's address
        so offset estimates stay relative to the authoritative clock.
        """
        leader_id = self.raft.get_leader()
        if leader_id == -1 or not self.peers:
            return
        if leader_id == self.node_id:
            # We are leader — use the first follower as reference
            self.time_syncer.set_reference(self.peers[0])
        else:
            idx = leader_id - 1
            if 0 <= idx < len(self.peers):
                self.time_syncer.set_reference(self.peers[idx])

    # ── server lifecycle ──────────────────────────────────────────────────

    def start(self):
        print(f"\n[HiveChat] ═══════════════════════════════════════════")
        print(f"[HiveChat] Starting Node {self.node_id} on port {self.port}")
        print(f"[HiveChat] Address : {self.address}")
        print(f"[HiveChat] Peers   : {self.peers or 'None (standalone)'}")
        print(f"[HiveChat] ═══════════════════════════════════════════\n")

        # ── Start gRPC server ─────────────────────────────────────────────
        self._grpc_server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10)
        )
        hivechat_pb2_grpc.add_MessagingServiceServicer_to_server(
            MessagingServicer(self), self._grpc_server
        )
        hivechat_pb2_grpc.add_FaultServiceServicer_to_server(
            FaultServicer(self), self._grpc_server
        )
        self._grpc_server.add_insecure_port(f"[::]:{self.port}")
        self._grpc_server.start()
        print(f"[HiveChat] ✓ gRPC server listening on port {self.port}")

        # ── Start Time Synchronization ────────────────────────────────────
        self.time_syncer.start()
        print(f"[HiveChat] ✓ Time Synchronization module started")

        # ── Start Fault Tolerance ─────────────────────────────────────────
        self.fault_manager.start()
        print(f"[HiveChat] ✓ Fault Tolerance module started")

        # ── In-process modules are immediately ready ──────────────────────
        print(f"[HiveChat] ✓ Data Replication module ready "
              f"(quorum_w={self.replication_manager.quorum_w})")
        print(f"[HiveChat] ✓ Raft Consensus module ready "
              f"(state={self.raft.get_state()})")
        print(f"\n[HiveChat] Node {self.node_id} is FULLY READY.\n")

    def stop(self):
        print(f"\n[HiveChat] Shutting down Node {self.node_id} …")
        self.fault_manager.stop()
        self.time_syncer.stop()
        if self._grpc_server:
            self._grpc_server.stop(grace=5)
        print(f"[HiveChat] Node {self.node_id} stopped.")

    def wait_for_termination(self):
        """Block until the gRPC server shuts down (Ctrl+C)."""
        if self._grpc_server:
            self._grpc_server.wait_for_termination()

    # ── public helpers ────────────────────────────────────────────────────

    def handle_client_message(self, sender: str, receiver: str, content: str) -> dict:
        message = self.fault_manager.build_message(sender, receiver, content)
        return self.fault_manager.handle_client_message(message)

    def handle_replica_message(self, message: dict) -> dict:
        return self.fault_manager.handle_replica_message(message)

    def export_messages(self):
        return self.fault_manager.export_messages()

    def get_peer_status(self):
        return self.fault_manager.get_peer_status()

    def get_metrics(self):
        base = self.fault_manager.get_metrics()
        base["raft_state"]   = self.raft.get_state()
        base["raft_leader"]  = self.raft.get_leader()
        base["raft_log_len"] = len(self.raft.log)
        base["time_sync"]    = self.time_syncer.get_stats()
        return base

    # ── interactive demo loop ─────────────────────────────────────────────

    def run_demo_loop(self):
        print("[Demo Mode]  Commands:")
        print("  send <sender> <receiver> <message>")
        print("  elect    – start Raft leader election on this node")
        print("  status   – show Raft state + time sync stats")
        print("  metrics | peers | messages | exit\n")

        while True:
            try:
                command = input(f"node{self.node_id}> ").strip()
                if not command:
                    continue

                if command == "exit":
                    break
                elif command == "metrics":
                    import json
                    print(json.dumps(self.get_metrics(), indent=2))
                elif command == "peers":
                    print(self.get_peer_status())
                elif command == "messages":
                    import json
                    print(json.dumps(self.export_messages(), indent=2))
                elif command == "elect":
                    won = self.raft.start_election()
                    print(f"[Raft] Election: {'WON – I am leader ' if won else 'LOST'}")
                    self._update_time_sync_reference()
                elif command == "status":
                    import json
                    leader_str = (f"node{self.raft.get_leader()}"
                                  if self.raft.get_leader() != -1 else "unknown")
                    print(f"[Raft] State  : {self.raft.get_state()}")
                    print(f"[Raft] Leader : {leader_str}")
                    print(f"[Raft] Log    : {len(self.raft.log)} entries")
                    print(f"[TimeSync]    : {json.dumps(self.time_syncer.get_stats(), indent=2)}")
                elif command.startswith("send "):
                    parts = command.split(" ", 3)
                    if len(parts) < 4:
                        print("Usage: send <sender> <receiver> <message>")
                        continue
                    result = self.handle_client_message(parts[1], parts[2], parts[3])
                    print(result)
                else:
                    print("Unknown command.")

            except KeyboardInterrupt:
                print("\n[HiveChat] Interrupted.")
                break
            except Exception as exc:
                print(f"[HiveChat] Error: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="HiveChat Node")
    parser.add_argument("--node-id", type=int, required=True)
    parser.add_argument("--port",    type=int, required=True)
    parser.add_argument(
        "--peers", type=str, default="",
        help="Comma-separated peer addresses, e.g. localhost:5002,localhost:5003"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="Run interactive demo loop instead of blocking on gRPC"
    )
    parser.add_argument(
        "--sync-port",
        type=int,
        default=0,
        help="Port for the TimeSyncService gRPC server (default: port + 1000)",
    )
    return parser.parse_args()


def main():
    args  = parse_args()
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]

    node = HiveChatNode(node_id=args.node_id, port=args.port, peers=peers)

    try:
        node.start()
        if args.demo:
            node.run_demo_loop()
        else:
            node.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


if __name__ == "__main__":
    main()