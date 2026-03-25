"""
HiveChat – Client
==================
Sends and receives messages through the HiveChat cluster.
Uses gRPC (MessagingService) and automatic failover:
  – tries the primary server first
  – if it fails, rotates through the backup list silently
"""

import argparse
import sys
import time
from pathlib import Path

import grpc

# ── path setup ────────────────────────────────────────────────────────────────
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from proto import hivechat_pb2, hivechat_pb2_grpc

# How long to wait for a server to respond before trying the next one
SEND_TIMEOUT    = 5.0
RECEIVE_TIMEOUT = 10.0
# Seconds to wait between failover attempts
RETRY_DELAY     = 0.5


# ─────────────────────────────────────────────────────────────────────────────
# Client
# ─────────────────────────────────────────────────────────────────────────────

class HiveChatClient:
    """
    Client with real gRPC transport and automatic failover.

    Failover strategy:
      1. Try each server in order.
      2. On any RPC error, wait RETRY_DELAY seconds and try the next.
      3. If all fail, raise RuntimeError so the caller can log and retry later.
    """

    def __init__(self, username: str, servers: list):
        self.username = username
        self.servers  = servers          # ordered list; first = primary
        self._current_index = 0         # tracks the last working server

    # ── connection info ───────────────────────────────────────────────────

    def connect(self):
        print(f"[HiveChat Client] User    : {self.username}")
        print(f"[HiveChat Client] Servers : {self.servers}")
        print(f"[HiveChat Client] Primary : {self.servers[self._current_index]}")

    # ── send with failover ────────────────────────────────────────────────

    def send_with_failover(self, recipient: str, content: str) -> dict:
        """
        Try each server in order starting from the last-known working one.
        Returns the server response dict on success.
        """
        last_error = None
        num_servers = len(self.servers)

        for attempt in range(num_servers):
            # rotate through the list, starting from _current_index
            idx    = (self._current_index + attempt) % num_servers
            server = self.servers[idx]

            try:
                result = self.send_message_to_server(server, recipient, content)
                self._current_index = idx          # remember the working server
                print(f"[FAILOVER] Delivered via {server}")
                return result
            except Exception as exc:
                print(f"[FAILOVER] {server} unavailable ({exc}). Trying next …")
                last_error = exc
                if attempt < num_servers - 1:
                    time.sleep(RETRY_DELAY)

        raise RuntimeError(f"All servers failed. Last error: {last_error}")

    def send_message_to_server(self, server: str, recipient: str, content: str) -> dict:
        """
        Open a gRPC channel to `server` and call SendMessage.
        Raises grpc.RpcError on any transport / server failure.
        """
        import uuid, time as _time
        with grpc.insecure_channel(server) as channel:
            stub    = hivechat_pb2_grpc.MessagingServiceStub(channel)
            request = hivechat_pb2.SendMessageRequest(
                message=hivechat_pb2.ChatMessage(
                    message_id  = str(uuid.uuid4()),
                    sender      = self.username,
                    receiver    = recipient,
                    content     = content,
                    timestamp   = _time.time(),
                    origin_node = "client",
                )
            )
            resp = stub.SendMessage(request, timeout=SEND_TIMEOUT)
            return {
                "status":     resp.status,
                "message_id": resp.message_id,
                "node_id":    resp.node_id,
                "server":     server,
            }

    # ── receive (inbox) ───────────────────────────────────────────────────

    def receive_messages(self) -> list:
        """
        Pull all messages from the cluster with automatic failover.
        Useful for a simple inbox view.
        """
        last_error = None
        num_servers = len(self.servers)

        for attempt in range(num_servers):
            idx    = (self._current_index + attempt) % num_servers
            server = self.servers[idx]
            try:
                with grpc.insecure_channel(server) as channel:
                    stub    = hivechat_pb2_grpc.MessagingServiceStub(channel)
                    request = hivechat_pb2.GetMessagesRequest(node_id="client")
                    resp    = stub.GetMessages(request, timeout=RECEIVE_TIMEOUT)
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
                        if m.receiver == self.username
                    ]
                    
                    if attempt > 0:
                        print(f"[FAILOVER] Inbox fetched from {server}")
                        self._current_index = idx  # update active server
                        
                    return messages
            except Exception as exc:
                print(f"[FAILOVER] {server} (inbox) unavailable ({exc}). Trying next …")
                last_error = exc
                if attempt < num_servers - 1:
                    time.sleep(RETRY_DELAY)

        print(f"[HiveChat Client] Inbox fetch failed: All servers unavailable. Last error: {last_error}")
        return []

    # ── interactive loop ──────────────────────────────────────────────────

    def run(self):
        self.connect()
        print("\n[HiveChat Client] Ready.")
        print("  @recipient <message>  – send a message")
        print("  /servers              – list configured servers")
        print("  /inbox                – check your messages")
        print("  /exit                 – quit\n")

        while True:
            try:
                user_input = input(f"{self.username}> ").strip()
                if not user_input:
                    continue

                if user_input == "/exit":
                    print("[HiveChat Client] Goodbye.")
                    break

                if user_input == "/servers":
                    print(f"  Servers: {self.servers}")
                    print(f"  Active : {self.servers[self._current_index]}")
                    continue

                if user_input == "/inbox":
                    msgs = self.receive_messages()
                    if msgs:
                        print(f"  {len(msgs)} message(s) for {self.username}:")
                        for m in msgs:
                            print(f"    [{m['sender']}] {m['content']}")
                    else:
                        print("  Inbox is empty.")
                    continue

                if user_input.startswith("@") and " " in user_input:
                    space     = user_input.index(" ")
                    recipient = user_input[1:space].strip()
                    content   = user_input[space + 1:].strip()
                    if recipient and content:
                        result = self.send_with_failover(recipient, content)
                        print(f"  ✓ Sent (id={result['message_id'][:8]}… via {result['node_id']})")
                        continue

                print("  Format: @recipient <message>  or  /command")

            except KeyboardInterrupt:
                print("\n[HiveChat Client] Interrupted.")
                break
            except RuntimeError as exc:
                print(f"  [ERROR] {exc}")
            except Exception as exc:
                print(f"  [ERROR] {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="HiveChat Client")
    parser.add_argument(
        "--server", type=str, default="localhost:5001",
        help="Primary server address"
    )
    parser.add_argument(
        "--servers", type=str, default="",
        help="Comma-separated failover list, e.g. localhost:5001,localhost:5002"
    )
    parser.add_argument("--user", type=str, required=True, help="Your username")
    return parser.parse_args()


def main():
    args    = parse_args()
    servers = [s.strip() for s in args.servers.split(",") if s.strip()]
    if not servers:
        servers = [args.server]

    print(f"[HiveChat Client] Connecting as '{args.user}' → {servers}")
    client = HiveChatClient(username=args.user, servers=servers)
    client.run()


if __name__ == "__main__":
    main()