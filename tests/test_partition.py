"""
Tests for network partition simulation.

Covers:
  - Isolating a minority node (partition)
  - Majority side can still commit entries
  - Minority side CANNOT commit entries
  - After partition heals, the minority node syncs missing logs
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
# Test: Basic Network Partition
# ─────────────────────────────────────────────────────────────────────────────

class TestNetworkPartition:
    """
    Simulate a network partition where one node is isolated from the rest.

    In a 3-node cluster split into {0, 1} and {2}:
      - The majority side {0, 1} can elect a leader and commit entries.
      - The minority node {2} cannot elect a leader or commit.

    WHY: Network partitions are a core failure mode in distributed systems.
         Raft must ensure that ONLY the majority partition can make progress,
         preventing split-brain.
    """

    def test_majority_side_elects_leader(self):
        """
        The majority side (2 out of 3) can hold a valid election.
        """
        nodes = create_cluster(3)

        # Partition: node 2 cannot reach 0 or 1
        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        # Node 0 starts election — should win with votes from 0 and 1
        result = nodes[0].start_election()
        assert result is True
        assert nodes[0].is_leader()

    def test_majority_side_can_commit(self):
        """
        After electing a leader, the majority side can commit entries
        because there are enough nodes (2/3) to form a majority.
        """
        mock_repl = MockReplication()
        nodes = create_cluster(3, replication=mock_repl)

        # Partition off node 2
        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        nodes[0].start_election()
        result = nodes[0].receive_client_message("majority_msg")

        assert result is True
        assert nodes[0].commit_index == 0
        assert len(mock_repl.applied_entries) == 1

    def test_minority_side_cannot_elect_leader(self):
        """
        The isolated node (minority of 1 out of 3) cannot get a majority
        vote, so it cannot become leader.
        """
        nodes = create_cluster(3)

        # Partition: node 2 is alone
        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        result = nodes[2].start_election()
        assert result is False
        assert nodes[2].is_leader() is False

    def test_minority_side_cannot_commit(self):
        """
        Even if the minority node were somehow a leader (stale), it
        should not be able to commit because it can't reach a majority.
        """
        nodes = create_cluster(3)

        # First, elect node 0 as leader before partition
        nodes[0].start_election()

        # Now partition: node 0 is isolated
        nodes[0].partition_from({1, 2})
        nodes[1].partition_from({0})
        nodes[2].partition_from({0})

        # Node 0 tries to write — cannot replicate to majority
        result = nodes[0].receive_client_message("isolated_msg")
        assert result is False


# ─────────────────────────────────────────────────────────────────────────────
# Test: Partition Healing
# ─────────────────────────────────────────────────────────────────────────────

class TestPartitionHealing:
    """
    When a network partition heals, the previously isolated node must
    sync its log from the current leader.

    WHY: After partition heals, all nodes must converge to the same log.
         The minority node missed entries during the partition and needs
         to catch up.
    """

    def test_node_syncs_after_partition_heals(self):
        """
        Scenario:
          1. Partition node 2 from {0, 1}.
          2. Node 0 becomes leader and commits entries.
          3. Heal the partition.
          4. Node 2 syncs from the leader.
          5. Node 2 should have all committed entries.
        """
        nodes = create_cluster(3)

        # Step 1: Partition
        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        # Step 2: Node 0 becomes leader, commits entries
        nodes[0].start_election()
        nodes[0].receive_client_message("during_partition")

        # Node 2 should have nothing
        assert len(nodes[2].get_log()) == 0

        # Step 3: Heal partition
        nodes[0].heal_partition()
        nodes[1].heal_partition()
        nodes[2].heal_partition()

        # Step 4: Node 2 syncs from leader
        nodes[2].sync_from_leader(nodes[0])

        # Step 5: Node 2 should now have the entry
        assert len(nodes[2].get_log()) == 1
        assert nodes[2].get_log()[0]["message"] == "during_partition"

    def test_partition_heal_with_multiple_entries(self):
        """
        The minority node should receive ALL entries it missed, in order.
        """
        nodes = create_cluster(3)

        # Partition off node 2
        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        # Leader sends multiple messages
        nodes[0].start_election()
        for msg in ["first", "second", "third"]:
            nodes[0].receive_client_message(msg)

        # Heal and sync
        nodes[0].heal_partition()
        nodes[1].heal_partition()
        nodes[2].heal_partition()
        nodes[2].sync_from_leader(nodes[0])

        # Verify all entries present and in order
        log = nodes[2].get_log()
        assert len(log) == 3
        assert log[0]["message"] == "first"
        assert log[1]["message"] == "second"
        assert log[2]["message"] == "third"

    def test_commit_index_syncs_after_partition_heals(self):
        """
        After syncing, the previously partitioned node's commit_index
        should match the leader's.
        """
        nodes = create_cluster(3)

        nodes[2].partition_from({0, 1})
        nodes[0].partition_from({2})
        nodes[1].partition_from({2})

        nodes[0].start_election()
        nodes[0].receive_client_message("sync_commit")

        # Heal
        for n in nodes:
            n.heal_partition()

        nodes[2].sync_from_leader(nodes[0])
        assert nodes[2].commit_index == nodes[0].commit_index


# ─────────────────────────────────────────────────────────────────────────────
# Test: Partition With Leader Change
# ─────────────────────────────────────────────────────────────────────────────

class TestPartitionWithLeaderChange:
    """
    Advanced scenario where the leader is on the minority side of a
    partition, and the majority side elects a new leader.
    """

    def test_majority_elects_new_leader_after_old_leader_partitioned(self):
        """
        Scenario:
          1. Node 0 is leader.
          2. Network splits: {0} vs {1, 2}.
          3. Node 1 starts election on the majority side → wins.
          4. Old leader (node 0) cannot commit.
        """
        nodes = create_cluster(3)
        nodes[0].start_election()
        assert nodes[0].is_leader()

        # Partition: node 0 is alone
        nodes[0].partition_from({1, 2})
        nodes[1].partition_from({0})
        nodes[2].partition_from({0})

        # Majority side elects a new leader
        result = nodes[1].start_election()
        assert result is True
        assert nodes[1].is_leader()

        # Old leader cannot commit
        result = nodes[0].receive_client_message("old_leader_msg")
        assert result is False

        # New leader can commit
        result = nodes[1].receive_client_message("new_leader_msg")
        assert result is True
