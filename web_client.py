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