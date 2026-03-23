"""
HiveChat - Main Server Node
Entry point for starting a distributed messaging node.
Includes integration for Fault Tolerance.
"""

import argparse
import sys
import time
from pathlib import Path

# Allow imports from project root when running:
# python node/server.py ...
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from node.fault import FaultToleranceManager


def parse_args():
    parser = argparse.ArgumentParser(description="HiveChat Node")
    parser.add_argument("--node-id", type=int, required=True, help="Unique node ID")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument(
        "--peers",
        type=str,
        default="",
        help="Comma-separated list of peer addresses (e.g. localhost:5002,localhost:5003)",
    )
    return parser.parse_args()


class HiveChatNode:
    """
    Main node wrapper for HiveChat.

    This class wires Fault Tolerance into the server lifecycle.
    Other modules such as consensus, replication, and time sync
    can later be connected here as well.
    """

    def __init__(self, node_id: int, port: int, peers: list[str]):
        self.node_id = node_id
        self.port = port
        self.address = f"localhost:{port}"
        self.peers = peers

        # Create data directory for persistent storage
        data_dir = PROJECT_ROOT / "node" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)

        store_path = str(data_dir / f"node{self.node_id}_messages.json")

        # Fault tolerance manager
        self.fault_manager = FaultToleranceManager(
            node_id=f"node{self.node_id}",
            peers=self.peers,
            heartbeat_fn=self.heartbeat_peer,
            replicate_fn=self.replicate_to_peer,
            fetch_messages_fn=self.fetch_messages_from_peer,
            store_path=store_path,
            replication_factor=2,
        )

    # ------------------------------------------------------------------
    # Placeholder integration points
    # Replace these later with your real transport / RPC / gRPC logic
    # ------------------------------------------------------------------

    def heartbeat_peer(self, peer_address: str) -> bool:
        """
        Check whether a peer is alive.

        Current version:
        - simple placeholder
        - assumes peer is alive if its address format is valid

        Later you should replace this with real heartbeat logic,
        for example a gRPC ping or socket health check.
        """
        try:
            if not peer_address:
                return False

            if ":" not in peer_address:
                return False

            host, port = peer_address.split(":", 1)
            if not host or not port:
                return False

            int(port)  # validate port format
            return True
        except Exception:
            return False

    def replicate_to_peer(self, peer_address: str, message: dict) -> bool:
        """
        Send a replica message to another peer.

        Current version:
        - placeholder for integration
        - prints replication activity
        - returns True to simulate successful replication

        Later replace with actual peer-to-peer communication.
        """
        try:
            print(
                f"[Replication] node{self.node_id} -> {peer_address} "
                f"(message_id={message['message_id']})"
            )
            return True
        except Exception:
            return False

    def fetch_messages_from_peer(self, peer_address: str):
        """
        Fetch all messages from a peer during recovery.

        Current version:
        - placeholder for integration
        - returns empty list

        Later replace with actual remote fetch logic.
        """
        try:
            print(f"[Recovery] Requesting messages from peer {peer_address}")
            return []
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Server lifecycle
    # ------------------------------------------------------------------

    def start(self):
        """
        Start all node services.
        """
        print(f"[HiveChat] Starting Node {self.node_id} on port {self.port}")
        print(f"[HiveChat] Address: {self.address}")
        print(f"[HiveChat] Peers: {self.peers if self.peers else 'None (standalone mode)'}")

        # TODO: Initialize components
        # 1. Time Sync
        print("[HiveChat] Initializing Time Sync...")

        # 2. Consensus (Raft)
        print("[HiveChat] Initializing Consensus module...")

        # 3. Data Replication
        print("[HiveChat] Initializing Replication module...")

        # 4. Fault Tolerance
        print("[HiveChat] Initializing Fault Tolerance module...")
        self.fault_manager.start()

        print(f"[HiveChat] Node {self.node_id} is ready.")

    def stop(self):
        """
        Stop node services gracefully.
        """
        print(f"[HiveChat] Shutting down Node {self.node_id}...")
        self.fault_manager.stop()
        print(f"[HiveChat] Node {self.node_id} stopped.")

    # ------------------------------------------------------------------
    # Fault tolerance helper methods
    # These are the methods your project can call from client/RPC layer
    # ------------------------------------------------------------------

    def handle_client_message(self, sender: str, receiver: str, content: str) -> dict:
        """
        Called when this node receives a new client message.
        """
        message = self.fault_manager.build_message(sender, receiver, content)
        result = self.fault_manager.handle_client_message(message)
        return result

    def handle_replica_message(self, message: dict) -> dict:
        """
        Called when another node sends a replica.
        """
        result = self.fault_manager.handle_replica_message(message)
        return result

    def export_messages(self):
        """
        Used by recovering nodes.
        """
        return self.fault_manager.export_messages()

    def get_peer_status(self):
        """
        View peer health.
        """
        return self.fault_manager.get_peer_status()

    def get_metrics(self):
        """
        View redundancy and recovery metrics.
        """
        return self.fault_manager.get_metrics()

    # ------------------------------------------------------------------
    # Demo loop
    # This is optional, but useful so your server does something visible
    # while you are still building the full project.
    # ------------------------------------------------------------------

    def run_demo_loop(self):
        """
        Simple interactive loop for testing fault tolerance manually.
        """
        print("\n[Demo Mode]")
        print("Commands:")
        print("  send <sender> <receiver> <message>")
        print("  metrics")
        print("  peers")
        print("  messages")
        print("  exit\n")

        while True:
            try:
                command = input(f"node{self.node_id}> ").strip()

                if not command:
                    continue

                if command == "exit":
                    break

                if command == "metrics":
                    print(self.get_metrics())
                    continue

                if command == "peers":
                    print(self.get_peer_status())
                    continue

                if command == "messages":
                    print(self.export_messages())
                    continue

                if command.startswith("send "):
                    parts = command.split(" ", 3)
                    if len(parts) < 4:
                        print("Usage: send <sender> <receiver> <message>")
                        continue

                    sender = parts[1]
                    receiver = parts[2]
                    content = parts[3]

                    result = self.handle_client_message(sender, receiver, content)
                    print(result)
                    continue

                print("Unknown command.")
            except KeyboardInterrupt:
                print("\n[HiveChat] Interrupted by user.")
                break
            except Exception as exc:
                print(f"[HiveChat] Error: {exc}")


def main():
    args = parse_args()
    peers = [p.strip() for p in args.peers.split(",") if p.strip()]

    node = HiveChatNode(
        node_id=args.node_id,
        port=args.port,
        peers=peers,
    )

    try:
        node.start()
        node.run_demo_loop()
    finally:
        node.stop()


if __name__ == "__main__":
    main()