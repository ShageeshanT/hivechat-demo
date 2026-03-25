"""
HiveChat - Main Server Node
Entry point for starting a distributed messaging node.
"""

import argparse
import logging

from node.time_sync import TimeSyncer
from node.time_sync_service import start_sync_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hivechat.server")


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
    parser.add_argument(
        "--sync-port",
        type=int,
        default=0,
        help="Port for the TimeSyncService gRPC server (default: port + 1000)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logger.info("Starting Node %d on port %d", args.node_id, args.port)

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    logger.info("Peers: %s", peers if peers else "None (standalone mode)")

    # ── 1. Time Synchronization ──────────────────────────────────────────
    # Start the gRPC time sync server so other nodes can query our clock
    sync_port = args.sync_port or (args.port + 1000)
    start_sync_server(args.node_id, sync_port)

    # Start the TimeSyncer client — syncs against the first peer by default
    reference_addr = f"{peers[0].split(':')[0]}:{int(peers[0].split(':')[1]) + 1000}" if peers else None
    time_syncer = TimeSyncer(node_id=args.node_id, reference_addr=reference_addr)
    time_syncer.start()

    logger.info("Time sync initialized (sync_port=%d, reference=%s)",
                sync_port, reference_addr or "none")

    # TODO: Initialize remaining components
    # 2. Consensus (Raft)
    # 3. Data Replication
    # 4. Fault Tolerance

    logger.info("Node %d is ready.", args.node_id)

    # Return syncer so other modules can access it
    return time_syncer


if __name__ == "__main__":
    main()
