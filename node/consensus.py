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


# RaftNode  –  The core consensus implementation

class RaftNode:
    """
    Simplified Raft consensus node.

    This class is designed to work in a *simulated* cluster where nodes are
    plain Python objects and RPCs are direct method calls.  No real networking
    is needed.

    Params:
        node_id      (int):           Unique ID of this node (e.g. 1, 2, 3).
        peers        (list[RaftNode]): References to all OTHER nodes in the cluster.
        replication  (object|None):   The replication module.  Must expose
                                      ``apply_committed_entry(entry_dict)``.
    """

    def __init__(self, node_id: int, peers: Optional[list] = None,
                 replication=None):
        # Identity
        self.node_id: int = node_id

        # Cluster topology
        # `peers` is a mutable list so we can add/remove nodes at runtime.
        self.peers: list["RaftNode"] = peers if peers is not None else []

        # Replication interface
        # RESPONSIBILITY: REPLICATION
        # The consensus module calls replication.apply_committed_entry()
        # after an entry has been committed by a majority.
        self.replication = replication

        # PERSISTENT STATE (on all servers)
        # These would normally be stored on stable storage so they survive
        # crashes.  For this simplified version, in-memory is fine.

        # current_term: Latest term this node has seen.
        # WHY: Terms act as a logical clock — they monotonically increase
        #      and help detect stale leaders / outdated messages.
        self.current_term: int = 0

        # voted_for: The candidate this node voted for in the current term.
        # WHY: Each node may vote for at most ONE candidate per term to
        #      guarantee that at most one leader can win per term.
        self.voted_for: Optional[int] = None

        # log: Ordered list of log entries.
        # WHY: The log is the heart of Raft.  All nodes must converge to
        #      the SAME log, in the SAME order.  The leader is the single
        #      source of truth.
        self.log: list[LogEntry] = []

        # VOLATILE STATE (on all servers)

        # commit_index: Index of the highest log entry known to be committed.
        # WHY: An entry is "committed" once a majority has replicated it.
        #      Only committed entries are safe to apply to the state machine.
        self.commit_index: int = -1

        # last_applied: Index of the highest log entry applied to the
        # replication state machine.
        # WHY: We need to track what has already been applied so we don't
        #      apply the same entry twice.
        self.last_applied: int = -1

        # VOLATILE STATE (leaders only — reinitialized after election)

        # next_index: For each peer, the index of the next log entry
        # the leader will send to that peer.
        # WHY: Used by the leader to track replication progress per follower.
        self.next_index: dict[int, int] = {}

        # match_index: For each peer, the index of the highest log entry
        # known to be replicated on that peer.
        # WHY: Used to calculate the commit point (majority).
        self.match_index: dict[int, int] = {}

        # NODE STATE

        # state: Current role of this node.
        self.state: NodeState = NodeState.FOLLOWER

        # leader_id: Who this node believes is the current leader.
        # -1 means "no known leader".
        self.leader_id: int = -1

        # SIMULATION HELPERS

        # active: When False, the node ignores all incoming RPCs.
        # WHY: Lets tests simulate crashes without removing the object.
        self.active: bool = True

        # partitioned_from: Set of node IDs this node cannot communicate with.
        # WHY: Simulates network partitions in tests.
        self.partitioned_from: set[int] = set()
