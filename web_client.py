import sys
import time
import logging
from pathlib import Path
from flask import Flask, render_template, request, jsonify

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root to path
CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import client
try:
    from client.client import HiveChatClient
except ImportError as e:
    logger.error(f"Failed to import HiveChatClient: {e}")
    sys.exit(1)

app = Flask(__name__)

# In-memory stores
clients = {}
online_users = {}  # {username: {"last_seen": timestamp, "servers": [...]}}

ONLINE_TIMEOUT = 30  # seconds before a user is considered offline


def get_client(username, servers_list):
    key = f"{username}:{','.join(servers_list)}"
    if key not in clients:
        logger.info(f"Creating new HiveChatClient for {username} with servers {servers_list}")
        clients[key] = HiveChatClient(username, servers_list)
    return clients[key]


def touch_user(username, servers=None):
    """Mark a user as online (update last_seen timestamp)."""
    if username:
        online_users[username] = {
            "last_seen": time.time(),
            "servers": servers or online_users.get(username, {}).get("servers", []),
        }


def get_online_list(exclude=None):
    """Return list of users seen within ONLINE_TIMEOUT seconds."""
    now = time.time()
    result = []
    for user, info in online_users.items():
        if user == exclude:
            continue
        if now - info["last_seen"] <= ONLINE_TIMEOUT:
            result.append({"username": user, "online": True})
        else:
            result.append({"username": user, "online": False})
    return result


# ---------- Routes ----------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/send", methods=["POST"])
def send_message():
    data = request.json
    username = data.get("username")
    recipient = data.get("recipient")
    content = data.get("content")
    servers = data.get("servers", ["localhost:5001", "localhost:5002", "localhost:5003"])

    if not username or not recipient or not content:
        return jsonify({"error": "Missing username, recipient, or content parameters"}), 400

    touch_user(username, servers)
    # Also register the recipient so they show up in the user list
    touch_user(recipient, servers)

    client = get_client(username, servers)
    try:
        result = client.send_with_failover(recipient, content)
        return jsonify({"success": True, "result": result})
    except Exception as e:
        logger.error(f"Error sending message: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/messages", methods=["GET"])
def get_messages():
    username = request.args.get("username")
    servers_param = request.args.get("servers", "localhost:5001,localhost:5002,localhost:5003")
    servers = [s.strip() for s in servers_param.split(",") if s.strip()]

    if not username:
        return jsonify({"error": "Username required"}), 400

    touch_user(username, servers)
    client = get_client(username, servers)
    try:
        messages = client.receive_messages()
        messages.sort(key=lambda x: x.get('timestamp', 0))
        return jsonify({"success": True, "messages": messages})
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/all-messages", methods=["GET"])
def get_all_messages():
    """Return ALL messages where user is sender OR receiver (for chat view)."""
    username = request.args.get("username")
    servers_param = request.args.get("servers", "localhost:5001,localhost:5002,localhost:5003")
    servers = [s.strip() for s in servers_param.split(",") if s.strip()]

    if not username:
        return jsonify({"error": "Username required"}), 400

    touch_user(username, servers)
    client = get_client(username, servers)
    try:
        # Get messages where user is receiver
        received = client.receive_messages()

        # Also get ALL messages from the cluster and filter for sent ones
        import grpc
        from proto import hivechat_pb2, hivechat_pb2_grpc

        all_msgs = []
        for server in servers:
            try:
                with grpc.insecure_channel(server) as channel:
                    stub = hivechat_pb2_grpc.MessagingServiceStub(channel)
                    req = hivechat_pb2.GetMessagesRequest(node_id="client")
                    resp = stub.GetMessages(req, timeout=10.0)
                    for m in resp.messages:
                        all_msgs.append({
                            "message_id": m.message_id,
                            "sender": m.sender,
                            "receiver": m.receiver,
                            "content": m.content,
                            "timestamp": m.timestamp,
                            "origin_node": m.origin_node,
                        })
                break  # Got from one server, no need to try others
            except Exception:
                continue

        # Deduplicate by message_id
        seen = set()
        unique = []
        for msg in all_msgs:
            if msg["message_id"] not in seen:
                seen.add(msg["message_id"])
                # Keep only messages involving this user
                if msg["sender"] == username or msg["receiver"] == username:
                    unique.append(msg)

        unique.sort(key=lambda x: x.get("timestamp", 0))
        return jsonify({"success": True, "messages": unique})
    except Exception as e:
        logger.error(f"Error fetching all messages: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/online-users", methods=["GET"])
def api_online_users():
    """Return list of all known users with their online status."""
    username = request.args.get("username")
    servers_param = request.args.get("servers", "")
    servers = [s.strip() for s in servers_param.split(",") if s.strip()]

    if username:
        touch_user(username, servers if servers else None)

    users = get_online_list(exclude=username)
    return jsonify({"success": True, "users": users})


@app.route("/api/heartbeat", methods=["POST"])
def heartbeat():
    """Keep a user marked as online."""
    data = request.json or {}
    username = data.get("username")
    servers = data.get("servers")
    if username:
        touch_user(username, servers)
    return jsonify({"success": True})


if __name__ == "__main__":
    logger.info("Starting HiveChat Web Interface on http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=True)
