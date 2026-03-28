"""
Tests for Raft leader election.

Covers:
  - A 3-node cluster elects exactly ONE leader
  - Majority vote is required to win
  - Term increments on each election
  - Stale candidates are rejected
  - Re-election after term advancement
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from node.consensus import RaftNode, NodeState


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def create_cluster(n: int = 3):
    """Create a cluster of `n` wired RaftNode objects."""
    nodes = [RaftNode(node_id=i) for i in range(n)]
    for node in nodes:
        node.set_peers([nd for nd in nodes if nd.node_id != node.node_id])
    return nodes


# ─────────────────────────────────────────────────────────────────────────────
# Test: Basic Leader Election
# ─────────────────────────────────────────────────────────────────────────────

class TestBasicLeaderElection:
    """
    Verify the fundamental Raft election guarantee:
    at most ONE leader per term, elected by a majority.
    """

    def test_single_leader_elected(self):
        """
        In a 3-node cluster, calling start_election on one node should
        produce exactly one leader.

        WHY: Raft's core invariant — one leader per term prevents
             conflicting decisions.
        """
        nodes = create_cluster(3)
        result = nodes[0].start_election()

        assert result is True
        assert nodes[0].is_leader() is True
        assert nodes[0].get_state() == "LEADER"

        # Other nodes should be followers
        assert nodes[1].get_state() == "FOLLOWER"
        assert nodes[2].get_state() == "FOLLOWER"

    def test_leader_id_propagated(self):
        """
        After election, ALL nodes should agree on who the leader is.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()

        # Leader knows its own ID
        assert nodes[0].get_leader() == 0

        # Followers learned the leader's ID via the heartbeat
        assert nodes[1].get_leader() == 0
        assert nodes[2].get_leader() == 0

    def test_term_incremented_on_election(self):
        """
        Starting an election MUST increment the term.
        WHY: Terms are Raft's logical clock — each election attempt starts
             a new term to distinguish it from previous attempts.
        """
        nodes = create_cluster(3)
        assert nodes[0].current_term == 0

        nodes[0].start_election()
        assert nodes[0].current_term == 1

    def test_candidate_votes_for_itself(self):
        """A candidate always votes for itself first."""
        nodes = create_cluster(3)
        nodes[0].start_election()
        assert nodes[0].voted_for == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: Majority Requirement
# ─────────────────────────────────────────────────────────────────────────────

class TestMajorityRequirement:
    """
    Raft requires a strict majority (> N/2) to elect a leader.
    This prevents split-brain: two majorities cannot both exist because
    they must overlap in at least one node.
    """

    def test_election_fails_without_majority(self):
        """
        If 2 out of 3 peers are down, the candidate cannot get a majority
        vote and the election must fail.
        """
        nodes = create_cluster(3)

        # Crash the other two nodes
        nodes[1].simulate_crash()
        nodes[2].simulate_crash()

        result = nodes[0].start_election()
        assert result is False
        assert nodes[0].is_leader() is False
        # Should revert to follower on failed election
        assert nodes[0].get_state() == "FOLLOWER"

    def test_election_with_one_peer_down(self):
        """
        With 1 of 3 nodes down, majority is still achievable (2/3).
        The election should succeed.
        """
        nodes = create_cluster(3)
        nodes[2].simulate_crash()

        result = nodes[0].start_election()
        assert result is True
        assert nodes[0].is_leader() is True

    def test_five_node_cluster_needs_three_votes(self):
        """
        In a 5-node cluster, majority = 3.  With 2 down, we have
        3 active nodes → election should succeed.
        """
        nodes = create_cluster(5)
        nodes[3].simulate_crash()
        nodes[4].simulate_crash()

        result = nodes[0].start_election()
        assert result is True

    def test_five_node_cluster_fails_with_three_down(self):
        """
        In a 5-node cluster with 3 down, only 2 nodes are alive.
        Majority = 3, so election must fail.
        """
        nodes = create_cluster(5)
        nodes[2].simulate_crash()
        nodes[3].simulate_crash()
        nodes[4].simulate_crash()

        result = nodes[0].start_election()
        assert result is False


# ─────────────────────────────────────────────────────────────────────────────
# Test: Vote Rejection Rules
# ─────────────────────────────────────────────────────────────────────────────

class TestVoteRejection:
    """
    Raft has strict rules about when a node may grant its vote.
    These tests verify the rejection conditions.
    """

    def test_reject_vote_for_stale_term(self):
        """
        A node in term 5 should reject a vote request from term 3.
        WHY: The candidate is behind — accepting it could revert progress.
        """
        nodes = create_cluster(3)
        # Manually advance node 1's term
        nodes[1].current_term = 5

        granted = nodes[1].request_vote(
            candidate_id=0,
            candidate_term=3,
            last_log_index=-1,
            last_log_term=0,
        )
        assert granted is False

    def test_reject_vote_if_already_voted(self):
        """
        If a node already voted for candidate A in this term, it must NOT
        vote for candidate B.
        WHY: Double-voting could lead to two leaders in the same term.
        """
        nodes = create_cluster(3)
        # Node 2 votes for node 0 in term 1
        nodes[2].request_vote(
            candidate_id=0, candidate_term=1,
            last_log_index=-1, last_log_term=0,
        )
        assert nodes[2].voted_for == 0

        # Node 2 should reject a vote for node 1 in the same term
        granted = nodes[2].request_vote(
            candidate_id=1, candidate_term=1,
            last_log_index=-1, last_log_term=0,
        )
        assert granted is False

    def test_grant_vote_in_new_higher_term(self):
        """
        If a candidate has a higher term, the voter should update its own
        term and grant the vote.
        """
        nodes = create_cluster(3)
        nodes[1].current_term = 1
        nodes[1].voted_for = 0  # Voted in term 1

        # New vote request in term 2 — should be granted
        granted = nodes[1].request_vote(
            candidate_id=2, candidate_term=2,
            last_log_index=-1, last_log_term=0,
        )
        assert granted is True
        assert nodes[1].voted_for == 2
        assert nodes[1].current_term == 2

    def test_inactive_node_does_not_vote(self):
        """Crashed nodes should not respond to vote requests."""
        nodes = create_cluster(3)
        nodes[1].simulate_crash()

        granted = nodes[1].request_vote(
            candidate_id=0, candidate_term=1,
            last_log_index=-1, last_log_term=0,
        )
        assert granted is False


# ─────────────────────────────────────────────────────────────────────────────
# Test: Sequential Elections
# ─────────────────────────────────────────────────────────────────────────────

class TestSequentialElections:
    """
    Verify that multiple elections work correctly over time
    (term advances, leader changes, etc.).
    """

    def test_second_election_higher_term(self):
        """
        A second election should produce a higher term number.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()
        assert nodes[0].current_term == 1

        # Node 1 starts a new election
        nodes[1].start_election()
        assert nodes[1].current_term == 2
        assert nodes[1].is_leader() is True
        # Node 0 should have stepped down
        assert nodes[0].is_leader() is False
