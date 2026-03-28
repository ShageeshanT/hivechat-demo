"""
Tests for leader failover and node rejoin.

Covers:
  - Leader crash triggers new election
  - A new leader is elected from surviving nodes
  - Rejoined node syncs missing log entries from the new leader
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
# Test: Leader Failover
# ─────────────────────────────────────────────────────────────────────────────

class TestLeaderFailover:
    """
    Simulate a leader crash and verify that the cluster elects a new leader.

    WHY this matters:
      Raft is designed for fault tolerance.  If the leader crashes, the
      remaining nodes must detect this (via timeout) and elect a new leader
      to maintain availability.
    """

    def test_new_leader_elected_after_crash(self):
        """
        Scenario:
          1. Node 0 becomes leader in term 1.
          2. Node 0 crashes.
          3. Node 1 starts a new election → becomes leader in term 2.
        """
        nodes = create_cluster(3)

        # Step 1: Node 0 wins the first election
        nodes[0].start_election()
        assert nodes[0].is_leader() is True

        # Step 2: Node 0 crashes
        nodes[0].simulate_crash()

        # Step 3: Node 1 starts a new election
        result = nodes[1].start_election()
        assert result is True
        assert nodes[1].is_leader() is True
        assert nodes[1].current_term == 2  # New term

    def test_crashed_leader_is_no_longer_leader(self):
        """A crashed node should not respond to any RPCs."""
        nodes = create_cluster(3)
        nodes[0].start_election()
        nodes[0].simulate_crash()

        # Node 0 is technically still in LEADER state, but it's inactive
        # and cannot serve requests.
        assert nodes[0].active is False
        assert nodes[0].receive_client_message("fail") is False

    def test_two_consecutive_failovers(self):
        """
        Scenario:
          1. Node 0 is leader (term 1) → crashes.
          2. Node 1 is elected (term 2) → crashes.
          3. Node 2 cannot become leader alone (only 1/3 alive).
        """
        nodes = create_cluster(3)

        nodes[0].start_election()
        assert nodes[0].is_leader()

        nodes[0].simulate_crash()
        nodes[1].start_election()
        assert nodes[1].is_leader()

        nodes[1].simulate_crash()
        # Node 2 tries but can't get majority (it's alone)
        result = nodes[2].start_election()
        assert result is False


# ─────────────────────────────────────────────────────────────────────────────
# Test: Node Rejoin
# ─────────────────────────────────────────────────────────────────────────────

class TestNodeRejoin:
    """
    After a failed node recovers, it must sync its log from the current
    leader to catch up on entries it missed while down.

    WHY: The rejoining node may have missed committed entries.  If it
         doesn't sync, its log will diverge from the rest of the cluster.
    """

    def test_rejoined_node_syncs_log(self):
        """
        Scenario:
          1. Node 0 is leader, sends 2 messages.
          2. Node 2 crashes BEFORE messages are sent.
          3. Node 2 recovers and syncs from leader.
          4. Node 2 should have the same log as Node 0.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        # Crash node 2 first so it misses the messages
        nodes[2].simulate_crash()

        # Leader sends messages (only node 0 and node 1 get them)
        nodes[0].receive_client_message("msg1")
        nodes[0].receive_client_message("msg2")

        # Verify node 2 has nothing
        assert len(nodes[2].get_log()) == 0

        # Node 2 recovers
        nodes[2].recover()

        # Sync from leader
        nodes[2].sync_from_leader(nodes[0])

        # Node 2 should now have both messages
        assert len(nodes[2].get_log()) == 2
        assert nodes[2].get_log()[0]["message"] == "msg1"
        assert nodes[2].get_log()[1]["message"] == "msg2"

    def test_rejoin_preserves_commit_index(self):
        """
        After syncing, the rejoined node's commit_index should match
        the leader's commit_index.
        """
        mock_repl = MockReplication()
        nodes = create_cluster(3, replication=mock_repl)
        nodes[0].start_election()

        nodes[2].simulate_crash()
        nodes[0].receive_client_message("committed_msg")

        nodes[2].recover()
        nodes[2].sync_from_leader(nodes[0])

        assert nodes[2].commit_index == nodes[0].commit_index

    def test_rejoin_after_new_leader_elected(self):
        """
        Scenario:
          1. Node 0 is leader, sends 1 message.
          2. Node 0 crashes.
          3. Node 1 becomes new leader, sends 1 more message.
          4. Node 0 recovers and syncs from Node 1.
          5. Node 0 should have both messages.
        """
        nodes = create_cluster(3)

        # Phase 1: Node 0 leads and sends a message
        nodes[0].start_election()
        nodes[0].receive_client_message("before_crash")

        # Phase 2: Node 0 crashes, Node 1 takes over
        nodes[0].simulate_crash()
        nodes[1].start_election()
        assert nodes[1].is_leader()

        nodes[1].receive_client_message("after_crash")

        # Phase 3: Node 0 recovers and syncs
        nodes[0].recover()
        nodes[0].sync_from_leader(nodes[1])

        # Should have the entry from the new leader
        log = nodes[0].get_log()
        messages = [e["message"] for e in log]
        assert "before_crash" in messages
        assert "after_crash" in messages
