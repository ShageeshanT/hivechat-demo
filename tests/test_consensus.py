"""
Tests for core RaftNode functionality.

Covers:
  - Node initialization and default state
  - Client message routing (leader accepts, follower rejects)
  - Integration with replication: apply_committed_entry is called ONLY after commit
"""

import sys
import os
import pytest

# Ensure the project root is on the path so we can import node.consensus
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from node.consensus import RaftNode, NodeState, LogEntry


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class MockReplication:
    """
    A lightweight mock of the replication module.

    Records every call to apply_committed_entry() so tests can verify
    that consensus hands off entries at the right time.
    """

    def __init__(self):
        self.applied_entries: list[dict] = []

    def apply_committed_entry(self, entry: dict) -> None:
        self.applied_entries.append(entry)


def create_cluster(n: int = 3, replication=None):
    """
    Create a cluster of `n` RaftNode objects wired together.

    Returns a list of nodes.  The first call to start_election() on any
    node will kick off leader election.
    """
    nodes = [RaftNode(node_id=i, replication=replication) for i in range(n)]
    for node in nodes:
        node.set_peers([n for n in nodes if n.node_id != node.node_id])
    return nodes


# ─────────────────────────────────────────────────────────────────────────────
# Test: Node Initialization
# ─────────────────────────────────────────────────────────────────────────────

class TestNodeInitialization:
    """Verify that a freshly created node has sane defaults."""

    def test_initial_state_is_follower(self):
        """Every node starts as a FOLLOWER — it hasn't heard from a leader yet."""
        node = RaftNode(node_id=0)
        assert node.get_state() == "FOLLOWER"

    def test_initial_term_is_zero(self):
        """Term 0 means no elections have occurred yet."""
        node = RaftNode(node_id=0)
        assert node.current_term == 0

    def test_initial_log_is_empty(self):
        """No log entries exist until a client writes a message."""
        node = RaftNode(node_id=0)
        assert node.get_log() == []

    def test_initial_leader_unknown(self):
        """No leader is known until an election succeeds."""
        node = RaftNode(node_id=0)
        assert node.get_leader() == -1

    def test_is_leader_false_initially(self):
        """A follower is not the leader."""
        node = RaftNode(node_id=0)
        assert node.is_leader() is False

    def test_initial_commit_index(self):
        """No entries are committed initially."""
        node = RaftNode(node_id=0)
        assert node.commit_index == -1

    def test_node_is_active_by_default(self):
        """Nodes are active (not crashed) when first created."""
        node = RaftNode(node_id=0)
        assert node.active is True


# ─────────────────────────────────────────────────────────────────────────────
# Test: Client Message Routing
# ─────────────────────────────────────────────────────────────────────────────

class TestClientMessageRouting:
    """Verify that only the leader accepts client writes."""

    def test_follower_rejects_client_write(self):
        """
        Followers MUST refuse client writes.
        WHY: In Raft, only the leader serializes writes to ensure consistency.
        """
        nodes = create_cluster(3)
        # No election → everyone is a follower
        assert nodes[0].receive_client_message("hello") is False

    def test_leader_accepts_client_write(self):
        """
        After a successful election, the leader should accept writes.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()
        assert nodes[0].is_leader() is True
        assert nodes[0].receive_client_message("hello") is True

    def test_inactive_node_rejects_write(self):
        """A crashed node should refuse all operations."""
        nodes = create_cluster(3)
        nodes[0].start_election()
        nodes[0].simulate_crash()
        assert nodes[0].receive_client_message("hello") is False


# ─────────────────────────────────────────────────────────────────────────────
# Test: Integration with Replication Module
# ─────────────────────────────────────────────────────────────────────────────

class TestReplicationIntegration:
    """
    Ensure consensus calls replication.apply_committed_entry() ONLY after
    an entry has been committed by a majority.

    RESPONSIBILITY: CONSENSUS decides when to commit.
                    REPLICATION stores the committed entry.
    """

    def test_apply_committed_entry_called_after_commit(self):
        """
        When a message is committed (majority replication succeeded),
        the replication module's apply_committed_entry must be invoked.
        """
        mock_repl = MockReplication()
        nodes = create_cluster(3, replication=mock_repl)
        nodes[0].start_election()

        # Send a message — expect it to be committed and applied
        nodes[0].receive_client_message("test message")

        # The mock should have recorded exactly 1 applied entry
        assert len(mock_repl.applied_entries) == 1
        assert mock_repl.applied_entries[0]["message"] == "test message"

    def test_no_apply_without_commit(self):
        """
        If the message cannot be committed (no majority), replication
        must NOT receive the entry.
        """
        mock_repl = MockReplication()
        nodes = create_cluster(3, replication=mock_repl)
        nodes[0].start_election()

        # Crash the followers so majority is impossible
        nodes[1].simulate_crash()
        nodes[2].simulate_crash()

        nodes[0].receive_client_message("should not commit")

        # Replication should NOT have been called
        assert len(mock_repl.applied_entries) == 0

    def test_multiple_entries_applied_in_order(self):
        """
        Multiple committed entries should be applied in log order.

        NOTE: We attach the mock to the leader only, because followers
        also call apply_committed_entry when they advance their commit_index.
        We want to verify the leader's apply behaviour specifically.
        """
        leader_repl = MockReplication()
        nodes = create_cluster(3)
        # Attach mock only to the leader node
        nodes[0].replication = leader_repl
        nodes[0].start_election()

        nodes[0].receive_client_message("first")
        nodes[0].receive_client_message("second")
        nodes[0].receive_client_message("third")

        assert len(leader_repl.applied_entries) == 3
        assert leader_repl.applied_entries[0]["message"] == "first"
        assert leader_repl.applied_entries[1]["message"] == "second"
        assert leader_repl.applied_entries[2]["message"] == "third"


# ─────────────────────────────────────────────────────────────────────────────
# Test: LogEntry
# ─────────────────────────────────────────────────────────────────────────────

class TestLogEntry:
    """Verify LogEntry serialization and equality."""

    def test_to_dict(self):
        entry = LogEntry(term=1, message="hello")
        assert entry.to_dict() == {"term": 1, "message": "hello"}

    def test_equality(self):
        a = LogEntry(term=1, message="hello")
        b = LogEntry(term=1, message="hello")
        assert a == b

    def test_inequality(self):
        a = LogEntry(term=1, message="hello")
        b = LogEntry(term=2, message="hello")
        assert a != b
