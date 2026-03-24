"""
HiveChat - Consensus Module (Raft)
Member: Gunitha

Implements a SIMPLIFIED Raft consensus algorithm for distributed leader
election, log replication, and commit decisions.

Key Raft concepts implemented:
  - Leader Election:   Timeout-based; candidate requests votes from peers.
  - Log Replication:   Leader appends entries and sends them to followers.
  - Commit Rule:       An entry is committed only when a MAJORITY of nodes
                       have acknowledged it.
  - State Machine:     After commit, entries are applied to the replication
                       module for storage and retrieval.

Architecture boundary:
  CONSENSUS is responsible for: election, log ordering, replication, commit.
  REPLICATION is responsible for: storing committed messages, deduplication.

Interface contract:
  Replication calls  →  consensus.get_leader(), consensus.is_leader()
  Consensus calls    →  replication.apply_committed_entry(entry)
"""

from enum import Enum
from typing import Optional

# Node States
# Every Raft node is in exactly one of these three states at any moment.
#   FOLLOWER  – Default state. Passively accepts RPCs from leaders/candidates.
#   CANDIDATE – Actively trying to become leader (started an election).
#   LEADER    – Coordinates the cluster; the ONLY node that accepts client writes.

class NodeState(Enum):
    """Possible states of a Raft node."""
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


# Log Entry
# Each entry records a client message alongside the leader's term when the
# entry was created.  The term is critical for consistency checks.

class LogEntry:
    """
    A single entry in the Raft replicated log.

    Attributes:
        term    (int): The leader's term when this entry was created.
        message (str): The client message payload.
    """

    def __init__(self, term: int, message: str):
        self.term = term
        self.message = message

    def to_dict(self) -> dict:
        """Serialize to a plain dict (useful for tests and replication)."""
        return {"term": self.term, "message": self.message}

    def __repr__(self) -> str:
        return f"LogEntry(term={self.term}, message={self.message!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LogEntry):
            return NotImplemented
        return self.term == other.term and self.message == other.message

