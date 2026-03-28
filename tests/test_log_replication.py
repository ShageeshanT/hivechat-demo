"""
Tests for Raft log replication and majority commit.

Covers:
  - Leader replicates entries to all followers
  - Entries are committed ONLY after majority acknowledgement
  - Without majority, entries remain uncommitted
  - All nodes maintain the same log order
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from node.consensus import RaftNode, NodeState


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

class MockReplication:
    """Mock replication module to track apply_committed_entry calls."""

    def __init__(self):
        self.applied_entries: list[dict] = []

    def apply_committed_entry(self, entry: dict) -> None:
        self.applied_entries.append(entry)


def create_cluster(n: int = 3, replication=None):
    nodes = [RaftNode(node_id=i, replication=replication) for i in range(n)]
    for node in nodes:
        node.set_peers([nd for nd in nodes if nd.node_id != node.node_id])
    return nodes


# ─────────────────────────────────────────────────────────────────────────────
# Test: Log Replication
# ─────────────────────────────────────────────────────────────────────────────

class TestLogReplication:
    """
    Verify that when the leader receives a client message, it replicates
    the entry to all followers.

    RESPONSIBILITY: CONSENSUS
    WHY: Every node must converge to the same log to maintain consistency.
    """

    def test_leader_appends_to_own_log(self):
        """The leader must first append the entry to its own log."""
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[0].receive_client_message("hello")

        log = nodes[0].get_log()
        assert len(log) == 1
        assert log[0]["message"] == "hello"
        assert log[0]["term"] == 1  # Leader elected in term 1

    def test_followers_receive_replicated_entries(self):
        """
        After the leader sends a message, all active followers should
        have the same entry in their logs.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[0].receive_client_message("replicated_msg")

        # All followers should have the entry
        for node in nodes:
            log = node.get_log()
            assert len(log) == 1, f"Node {node.node_id} has {len(log)} entries"
            assert log[0]["message"] == "replicated_msg"

    def test_all_nodes_same_log_order(self):
        """
        After multiple messages, all nodes must have the SAME entries
        in the SAME order.

        WHY: Raft guarantees a total order on the log.  If logs diverge,
             the system would deliver inconsistent results.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        messages = ["alpha", "beta", "gamma", "delta"]
        for msg in messages:
            nodes[0].receive_client_message(msg)

        # Every node should have the same 4 entries in order
        expected_log = nodes[0].get_log()
        for node in nodes[1:]:
            assert node.get_log() == expected_log, \
                f"Node {node.node_id} log differs from leader"

    def test_inactive_follower_does_not_receive_entry(self):
        """
        A crashed follower cannot receive the entry.  It will have an
        empty log while the active nodes have the entry.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[2].simulate_crash()
        nodes[0].receive_client_message("missed_by_node2")

        # Node 0 and 1 have the entry
        assert len(nodes[0].get_log()) == 1
        assert len(nodes[1].get_log()) == 1

        # Node 2 missed it
        assert len(nodes[2].get_log()) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: Majority Commit
# ─────────────────────────────────────────────────────────────────────────────

class TestMajorityCommit:
    """
    Verify the Raft commit rule: an entry is committed ONLY when a
    majority of nodes have it in their logs.

    WHY: Majority-based commit ensures that ANY future majority will
         include at least one node that has the committed entry.
         This is what makes committed entries durable.
    """

    def test_entry_committed_with_full_cluster(self):
        """
        With all 3 nodes alive, majority (2) is easily achieved.
        The leader's commit_index should advance.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[0].receive_client_message("all_alive")
        assert nodes[0].commit_index == 0  # First entry committed

    def test_entry_committed_with_one_follower_down(self):
        """
        With 1/3 nodes down, majority (2) is still achievable.
        Leader + 1 follower = 2 nodes = majority of 3.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()
        nodes[2].simulate_crash()

        result = nodes[0].receive_client_message("two_alive")
        assert result is True
        assert nodes[0].commit_index == 0

    def test_no_commit_without_majority(self):
        """
        If 2 out of 3 followers are down, the leader alone (1/3) cannot
        form a majority.  The entry MUST NOT be committed.

        WHY: Committing without majority could lose data — if the leader
             also crashes, no surviving node would have the entry.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        # Crash both followers
        nodes[1].simulate_crash()
        nodes[2].simulate_crash()

        result = nodes[0].receive_client_message("no_majority")
        assert result is False
        assert nodes[0].commit_index == -1  # Nothing committed

    def test_commit_index_advances_correctly(self):
        """
        After multiple committed entries, commit_index should point
        to the last committed entry.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[0].receive_client_message("entry_0")
        assert nodes[0].commit_index == 0

        nodes[0].receive_client_message("entry_1")
        assert nodes[0].commit_index == 1

        nodes[0].receive_client_message("entry_2")
        assert nodes[0].commit_index == 2

    def test_followers_commit_index_follows_leader(self):
        """
        After replication, followers should also advance their commit_index
        to match the leader's (via leader_commit in AppendEntries).
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        nodes[0].receive_client_message("msg1")
        nodes[0].receive_client_message("msg2")

        # Leader's commit_index should be 1
        assert nodes[0].commit_index == 1

        # Followers should have caught up on commit_index
        # (They learn via leader_commit in AppendEntries RPCs)
        # The followers' commit_index update happens during the
        # AppendEntries for the NEXT message, so after msg2:
        for node in nodes[1:]:
            assert node.commit_index >= 0, \
                f"Node {node.node_id} commit_index should advance"


# ─────────────────────────────────────────────────────────────────────────────
# Test: Log Entry Term
# ─────────────────────────────────────────────────────────────────────────────

class TestLogEntryTerm:
    """Verify that log entries record the correct term."""

    def test_entry_has_correct_term(self):
        """
        Each log entry should be tagged with the leader's current term.
        WHY: Terms are essential for the consistency checks in AppendEntries.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()  # Term 1

        nodes[0].receive_client_message("term1_msg")
        assert nodes[0].get_log()[0]["term"] == 1

    def test_entries_across_terms(self):
        """
        If different leaders write in different terms, the entries
        should reflect the term of the leader that wrote them.
        """
        nodes = create_cluster(3)

        # Term 1: Node 0 is leader, writes a message
        nodes[0].start_election()
        nodes[0].receive_client_message("term1")

        # Term 2: Node 1 takes over, writes a message
        nodes[1].start_election()
        nodes[1].receive_client_message("term2")

        # Node 1's log should have both entries with correct terms
        log = nodes[1].get_log()
        assert log[0]["term"] == 1
        assert log[1]["term"] == 2
