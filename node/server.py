"""
HiveChat – Main Server Node
============================
Entry point for starting a distributed messaging node.
Integrates:
  • Fault Tolerance  (Member 2 – Sihan)
  • gRPC server      (MessagingService + FaultService)

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

# ── generated gRPC stubs ──────────────────────────────────────────────────────
from proto import hivechat_pb2, hivechat_pb2_grpc
from node.fault import FaultToleranceManager

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

        # Let fault manager handle storage + replication
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
# HiveChatNode
# ─────────────────────────────────────────────────────────────────────────────

class HiveChatNode:
    """
    Main node wrapper for HiveChat.

    Wires the gRPC server together with the Fault Tolerance module.
    Other modules (Consensus, Replication, Time Sync) plug in here later.
    """

    def __init__(self, node_id: int, port: int, peers: list):
        self.node_id = node_id
        self.port    = port
        self.address = f"localhost:{port}"
        self.peers   = peers

        # Persistent storage path for this node
        data_dir = PROJECT_ROOT / "node" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        store_path = str(data_dir / f"node{self.node_id}_messages.json")

        # ── Fault Tolerance Manager ───────────────────────────────────────
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

        # ── gRPC server (created in start()) ─────────────────────────────
        self._grpc_server = None

    # ── gRPC transport helpers ────────────────────────────────────────────
    # These replace the old placeholders with real RPC calls.

    def _heartbeat_peer(self, peer_address: str) -> bool:
        """
        Real heartbeat: open a short-lived gRPC channel, call Heartbeat RPC.
        Returns True only if the peer responds alive=True within timeout.
        """
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
        """
        Real replication: open a gRPC channel, call Replicate RPC.
        Returns True on success.
        """
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
        """
        Real recovery fetch: call GetMessages RPC and convert proto → dicts.
        """
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

    # ── server lifecycle ──────────────────────────────────────────────────

    def start(self):
        print(f"[HiveChat] Starting Node {self.node_id} on port {self.port}")
        print(f"[HiveChat] Address : {self.address}")
        print(f"[HiveChat] Peers   : {self.peers or 'None (standalone)'}")

        # ── TODO placeholders for other members ──────────────────────────
        print("[HiveChat] [TODO] Time Sync module …")
        print("[HiveChat] [TODO] Consensus (Raft) module …")
        print("[HiveChat] [TODO] Data Replication module …")

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
        print(f"[HiveChat] gRPC server listening on port {self.port}")

        # ── Start Fault Tolerance ─────────────────────────────────────────
        print("[HiveChat] Initializing Fault Tolerance module …")
        self.fault_manager.start()

        print(f"[HiveChat] Node {self.node_id} is ready.\n")

    def stop(self):
        print(f"\n[HiveChat] Shutting down Node {self.node_id} …")
        self.fault_manager.stop()
        if self._grpc_server:
            self._grpc_server.stop(grace=5)
        print(f"[HiveChat] Node {self.node_id} stopped.")

    def wait_for_termination(self):
        """Block until the gRPC server shuts down (Ctrl+C)."""
        if self._grpc_server:
            self._grpc_server.wait_for_termination()

    # ── public helpers (used by demo loop / other modules) ───────────────

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
        return self.fault_manager.get_metrics()

    # ── interactive demo loop ─────────────────────────────────────────────

    def run_demo_loop(self):
        print("[Demo Mode]  Commands:")
        print("  send <sender> <receiver> <message>")
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