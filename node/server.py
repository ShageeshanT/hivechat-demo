"""
HiveChat - Main Server Node
Entry point for starting a distributed messaging node.
"""

import argparse


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


def main():
    args = parse_args()
    print(f"[HiveChat] Starting Node {args.node_id} on port {args.port}")

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    print(f"[HiveChat] Peers: {peers if peers else 'None (standalone mode)'}")

    # TODO: Initialize components
    # 1. Time Sync
    # 2. Consensus (Raft)
    # 3. Data Replication
    # 4. Fault Tolerance

    print(f"[HiveChat] Node {args.node_id} is ready.")


if __name__ == "__main__":
    main()
