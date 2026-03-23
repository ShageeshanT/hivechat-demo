"""
HiveChat - Client
Simple client to send and receive messages through the HiveChat cluster.
Includes automatic failover support for fault tolerance.
"""

import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="HiveChat Client")
    parser.add_argument(
        "--server",
        type=str,
        default="localhost:5001",
        help="Primary server address to connect to",
    )
    parser.add_argument(
        "--servers",
        type=str,
        default="",
        help="Comma-separated list of server addresses for failover "
        "(example: localhost:5001,localhost:5002,localhost:5003)",
    )
    parser.add_argument("--user", type=str, required=True, help="Your username")
    return parser.parse_args()


class HiveChatClient:
    """
    Client with simple failover support.

    Current version:
    - simulates sending through the listed servers
    - retries next server if one fails

    Later you can replace the placeholder send logic
    with real gRPC communication.
    """

    def __init__(self, username: str, servers: list[str]):
        self.username = username
        self.servers = servers

    def connect(self):
        """
        Show configured servers.
        In a real version, this would create gRPC stubs/channels.
        """
        print(f"[HiveChat Client] User: {self.username}")
        print(f"[HiveChat Client] Server list: {self.servers}")

    def send_with_failover(self, recipient: str, content: str):
        """
        Try each server in order until one succeeds.
        """
        last_error = None

        for server in self.servers:
            try:
                result = self.send_message_to_server(server, recipient, content)
                print(
                    f"[FAILOVER] Message delivered through {server}"
                )
                print(result)
                return result
            except Exception as exc:
                print(f"[FAILOVER] {server} is unavailable. Trying next server...")
                last_error = exc

        raise RuntimeError(f"All servers failed. Last error: {last_error}")

    def send_message_to_server(self, server: str, recipient: str, content: str):
        """
        Placeholder transport method.

        Replace this later with your actual gRPC request code.

        Example future use:
        - open gRPC channel
        - call SendMessage RPC
        - return server response
        """
        if not server or ":" not in server:
            raise ConnectionError(f"Invalid server address: {server}")

        # Simulated success
        return {
            "status": "sent",
            "server": server,
            "sender": self.username,
            "receiver": recipient,
            "content": content,
        }

    def receive_messages(self):
        """
        Placeholder for future receive logic.
        """
        print("[HiveChat Client] Inbox check is not implemented yet.")

    def run(self):
        """
        Interactive client loop.
        """
        self.connect()
        print("[HiveChat Client] Ready.")
        print("Type a message using this format:")
        print("  @recipient your message here")
        print("Commands:")
        print("  /servers   -> show configured servers")
        print("  /inbox     -> check inbox placeholder")
        print("  /exit      -> quit")
        print()

        while True:
            try:
                user_input = input(f"{self.username}> ").strip()

                if not user_input:
                    continue

                if user_input == "/exit":
                    print("[HiveChat Client] Exiting.")
                    break

                if user_input == "/servers":
                    print(f"[HiveChat Client] Servers: {self.servers}")
                    continue

                if user_input == "/inbox":
                    self.receive_messages()
                    continue

                if not user_input.startswith("@"):
                    print("Invalid format. Use: @recipient message")
                    continue

                if " " not in user_input:
                    print("Invalid format. Use: @recipient message")
                    continue

                first_space = user_input.find(" ")
                recipient = user_input[1:first_space].strip()
                content = user_input[first_space + 1 :].strip()

                if not recipient:
                    print("Recipient cannot be empty.")
                    continue

                if not content:
                    print("Message cannot be empty.")
                    continue

                self.send_with_failover(recipient, content)

            except KeyboardInterrupt:
                print("\n[HiveChat Client] Interrupted by user.")
                break
            except Exception as exc:
                print(f"[HiveChat Client] Error: {exc}")


def main():
    args = parse_args()

    servers = [s.strip() for s in args.servers.split(",") if s.strip()]

    if not servers:
        servers = [args.server]

    print(
        f"[HiveChat Client] Connecting to cluster as '{args.user}' "
        f"using servers: {servers}"
    )

    client = HiveChatClient(
        username=args.user,
        servers=servers,
    )
    client.run()


if __name__ == "__main__":
    main()