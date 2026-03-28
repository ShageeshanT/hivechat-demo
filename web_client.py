import sys
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

# Basic in-memory store for client instances
clients = {}

def get_client(username, servers_list):
    key = f"{username}:{','.join(servers_list)}"
    if key not in clients:
        logger.info(f"Creating new HiveChatClient for {username} with servers {servers_list}")
        clients[key] = HiveChatClient(username, servers_list)
    return clients[key]

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
        
    client = get_client(username, servers)
    try:
        messages = client.receive_messages()
        # Sort messages by timestamp
        messages.sort(key=lambda x: x.get('timestamp', 0))
        return jsonify({"success": True, "messages": messages})
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    logger.info("Starting HiveChat Web Interface on http://localhost:8000")
    app.run(host="0.0.0.0", port=8000, debug=True)