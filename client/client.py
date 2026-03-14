"""
HiveChat - Client
Simple client to send and receive messages through the HiveChat cluster.
"""

import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="HiveChat Client")
    parser.add_argument(
        "--server", type=str, default="localhost:5001", help="Server address to connect to"
    )
    parser.add_argument("--user", type=str, required=True, help="Your username")
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"[HiveChat Client] Connecting to {args.server} as '{args.user}'")

    # TODO: Connect to server via gRPC
    # TODO: Send/receive message loop

    print("[HiveChat Client] Ready. Type a message (format: @recipient message)")


if __name__ == "__main__":
    main()
