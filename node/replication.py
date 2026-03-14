"""
HiveChat - Data Replication Module
Handles message replication, consistency, and deduplication.
"""

class ReplicationManager:
    def __init__(self):
        self.messages = []      # Store replicated messages
        self.seen_ids = set()   # Track IDs to avoid duplicates

    def replicate_message(self, msg_id: str, content: str) -> bool:
        """Deduplicates and stores a message."""
        if msg_id in self.seen_ids:
            return False  # Already seen, ignore
            
        self.seen_ids.add(msg_id)
        self.messages.append({"id": msg_id, "content": content})
        return True
