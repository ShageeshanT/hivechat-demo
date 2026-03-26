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

    # PUBLIC INTERFACE  —  called by the REPLICATION module

    def get_leader(self) -> int:
        """
        Return the node_id of the current leader, or -1 if unknown.

        RESPONSIBILITY: CONSENSUS
        WHY: The replication module needs to know who the leader is so it
             can route client writes to the correct node.
        """
        return self.leader_id

    def is_leader(self) -> bool:
        """
        Return True if THIS node is the current leader.

        RESPONSIBILITY: CONSENSUS
        WHY: Used by the replication layer to decide if this node should
             accept client write requests.
        """
        return self.state == NodeState.LEADER

    # HELPER / TESTING METHODS

    def get_state(self) -> str:
        """Return the current node state as a string (for testing/debugging)."""
        return self.state.value

    def get_log(self) -> list[dict]:
        """Return the log as a list of dicts (for testing/debugging)."""
        return [entry.to_dict() for entry in self.log]

    # LEADER ELECTION  (§5.2 of the Raft paper)
    # WHY leader election exists:
    #   In a distributed system we need exactly ONE coordinator (the leader)
    #   to serialize client writes and ensure all nodes see the same log.
    #   Without a leader, concurrent writes could conflict.
    #
    # HOW it works:
    #   1. A follower times out (hasn't heard from a leader).
    #   2. It becomes a CANDIDATE, increments its term, votes for itself.
    #   3. It sends RequestVote RPCs to every peer.
    #   4. If it receives votes from a majority, it becomes LEADER.
    #   5. If another node has already won this term, it steps down.

    def start_election(self) -> bool:
        """
        Initiate a leader election on this node.

        This method transitions the node to CANDIDATE, increments the term,
        votes for itself, and solicits votes from all reachable peers.

        Returns:
            True  if this node won the election (became leader).
            False if the election failed (did not get majority).

        RESPONSIBILITY: CONSENSUS
        """
        # Guard: only active nodes can start elections
        if not self.active:
            return False

        # Step 1: Transition to CANDIDATE
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id  # Vote for ourselves

        # Track votes received; we already have our own vote.
        votes_received: int = 1
        total_nodes: int = len(self.peers) + 1  # peers + self
        majority_needed: int = (total_nodes // 2) + 1

        # Step 2: Determine our last log info for vote request
        # WHY: Raft requires the candidate to share its last log entry so
        #      voters can reject candidates with incomplete logs (§5.4.1).
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if self.log else 0

        # Step 3: Request votes from all reachable peers
        for peer in self.peers:
            # Skip unreachable / crashed / partitioned nodes
            if not peer.active:
                continue
            if peer.node_id in self.partitioned_from:
                continue
            if self.node_id in peer.partitioned_from:
                continue

            vote_granted = peer.request_vote(
                candidate_id=self.node_id,
                candidate_term=self.current_term,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            if vote_granted:
                votes_received += 1

        # Step 4: Did we win?
        # WHY: A candidate wins only with a STRICT MAJORITY.  This ensures
        #      at most one leader per term because any two majorities must
        #      overlap in at least one node.
        if votes_received >= majority_needed:
            self._become_leader()
            return True
        else:
            # Election failed — revert to follower.
            self.state = NodeState.FOLLOWER
            return False

    def request_vote(self, candidate_id: int, candidate_term: int,
                     last_log_index: int, last_log_term: int) -> bool:
        """
        Handle a RequestVote RPC from a candidate.

        This implements the Raft voting rules (§5.2, §5.4.1):
          1. Reject if candidate_term < current_term.
          2. Grant vote only if we haven't voted for someone else this term.
          3. Grant vote only if candidate's log is at least as up-to-date
             as ours (the "election restriction").

        Args:
            candidate_id:    Node ID of the candidate.
            candidate_term:  The candidate's current term.
            last_log_index:  Index of candidate's last log entry.
            last_log_term:   Term of candidate's last log entry.

        Returns:
            True if vote is granted, False otherwise.

        RESPONSIBILITY: CONSENSUS
        """
        # Guard: inactive nodes cannot vote
        if not self.active:
            return False

        # Rule 1: Reject stale candidates
        # WHY: A candidate in an older term is outdated and should not
        #      become leader.
        if candidate_term < self.current_term:
            return False

        # Update term if candidate has a newer term
        # WHY: If we see a higher term, there must have been an election
        #      we missed.  We step down to follower and update our term.
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self.leader_id = -1

        # Rule 2: Only one vote per term
        # WHY: If we already voted for a different candidate this term,
        #      we must not vote again to prevent split-brain.
        if self.voted_for is not None and self.voted_for != candidate_id:
            return False

        # Rule 3: Election restriction (log completeness)
        # WHY: We must not elect a leader that is MISSING committed entries.
        #      The candidate's log must be at least as up-to-date as ours.
        #      "Up-to-date" means:
        #        - Last entry has a HIGHER term, OR
        #        - Same term but log is at least as LONG.
        my_last_log_term = self.log[-1].term if self.log else 0
        my_last_log_index = len(self.log) - 1

        candidate_log_ok = (
            last_log_term > my_last_log_term or
            (last_log_term == my_last_log_term and
             last_log_index >= my_last_log_index)
        )

        if not candidate_log_ok:
            return False

        # Grant the vote
        self.voted_for = candidate_id
        return True

    def _become_leader(self) -> None:
        """
        Transition this node to LEADER state.

        Called when the node wins an election.  Initializes leader-only
        volatile state (next_index, match_index) and immediately sends
        empty heartbeats to all followers to establish authority.

        RESPONSIBILITY: CONSENSUS
        WHY: After winning, the leader must immediately assert itself so
             followers know to stop their election timers.
        """
        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        # Initialize leader-only state
        # next_index[peer] = len(log) → optimistically assume peers are
        #   up-to-date.  If not, we'll decrement on conflict.
        # match_index[peer] = -1 → we haven't confirmed anything yet.
        for peer in self.peers:
            self.next_index[peer.node_id] = len(self.log)
            self.match_index[peer.node_id] = -1

        # Send initial heartbeat (empty AppendEntries)
        # WHY: This prevents other nodes from timing out and starting
        #      their own elections.
        self._send_heartbeats()

    def _send_heartbeats(self) -> None:
        """
        Send empty AppendEntries RPCs to all peers (heartbeat).

        RESPONSIBILITY: CONSENSUS
        WHY: Heartbeats prevent followers from timing out and starting
             unnecessary elections.  They also carry the leader's commit
             index so followers can advance their own commit state.
        """
        for peer in self.peers:
            if not peer.active:
                continue
            if peer.node_id in self.partitioned_from:
                continue
            if self.node_id in peer.partitioned_from:
                continue

            prev_log_index = self.next_index.get(peer.node_id, 0) - 1
            prev_log_term = (
                self.log[prev_log_index].term
                if 0 <= prev_log_index < len(self.log) else 0
            )

            peer.append_entries(
                leader_id=self.node_id,
                term=self.current_term,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=[],  # Heartbeat — no entries
                leader_commit=self.commit_index,
            )

    # LOG REPLICATION  (§5.3 of the Raft paper)
    # HOW log replication works:
    #   1. Client sends a message to the leader.
    #   2. Leader appends it to its own log.
    #   3. Leader sends AppendEntries RPCs to every follower with the new
    #      entry (and possibly older ones the follower is missing).
    #   4. Each follower validates the entry and appends it to its own log.
    #   5. Once a MAJORITY of nodes (including the leader) have the entry,
    #      the leader commits it and applies it to the state machine.
    #
    # RESPONSIBILITY: CONSENSUS handles steps 1-5.
    #                 REPLICATION handles storage after commit (step 5).

    def receive_client_message(self, message: str) -> bool:
        """
        Handle a client write request.

        Only the LEADER may accept client writes.  Followers and candidates
        must reject them (the client should retry on the leader).

        Args:
            message: The client's message payload (a string).

        Returns:
            True  if the message was committed (majority acknowledged).
            False if the write was rejected or could not be committed.

        RESPONSIBILITY: CONSENSUS
        WHY: Centralizing writes through the leader ensures a single
             serialization point — all nodes see the same log order.
        """
        # Guard: only active leaders accept writes
        if not self.active:
            return False
        if self.state != NodeState.LEADER:
            return False

        # Step 1: Append to leader's own log
        entry = LogEntry(term=self.current_term, message=message)
        self.log.append(entry)

        # Step 2: Replicate to followers
        self.replicate_log()

        # Step 3: Try to commit
        self.commit_entries()

        # Return True if the new entry actually got committed.
        return self.commit_index >= len(self.log) - 1

    def replicate_log(self) -> None:
        """
        Send AppendEntries RPCs to all followers with any log entries
        they are missing.

        For each peer, we send entries starting from ``next_index[peer]``
        up to the end of our log.  If a peer rejects (log mismatch), we
        decrement ``next_index`` and retry — this is the standard Raft
        log backtracking mechanism.

        RESPONSIBILITY: CONSENSUS
        WHY: The leader must replicate every entry to every follower so
             the cluster maintains a consistent, ordered log.
        """
        if self.state != NodeState.LEADER:
            return

        for peer in self.peers:
            # Skip unreachable nodes
            if not peer.active:
                continue
            if peer.node_id in self.partitioned_from:
                continue
            if self.node_id in peer.partitioned_from:
                continue

            self._send_append_entries_to_peer(peer)

    def _send_append_entries_to_peer(self, peer: "RaftNode") -> None:
        """
        Send AppendEntries to a single peer, with backtracking on failure.

        RESPONSIBILITY: CONSENSUS
        WHY: If the follower's log diverges (e.g. it missed some entries),
             we must backtrack next_index until we find a matching point,
             then re-send from there.
        """
        # Retry loop handles log backtracking
        while True:
            next_idx = self.next_index.get(peer.node_id, 0)

            # Determine the previous entry's index/term for consistency check
            prev_log_index = next_idx - 1
            prev_log_term = (
                self.log[prev_log_index].term
                if 0 <= prev_log_index < len(self.log) else 0
            )

            # Entries to send: everything from next_idx onwards
            entries_to_send = self.log[next_idx:]

            success = peer.append_entries(
                leader_id=self.node_id,
                term=self.current_term,
                prev_log_index=prev_log_index,
                prev_log_term=prev_log_term,
                entries=entries_to_send,
                leader_commit=self.commit_index,
            )

            if success:
                # Update next_index and match_index on success
                if entries_to_send:
                    self.next_index[peer.node_id] = next_idx + len(entries_to_send)
                    self.match_index[peer.node_id] = self.next_index[peer.node_id] - 1
                break
            else:
                # Backtrack: decrement next_index and retry
                # WHY: The follower's log doesn't match at prev_log_index.
                #      We step back one entry and try again.
                if next_idx > 0:
                    self.next_index[peer.node_id] = next_idx - 1
                else:
                    break  # Can't go back further

    def append_entries(self, leader_id: int, term: int,
                       prev_log_index: int, prev_log_term: int,
                       entries: list, leader_commit: int) -> bool:
        """
        Handle an AppendEntries RPC (called on followers by the leader).

        This serves two purposes:
          1. HEARTBEAT: When `entries` is empty, it resets the election timer.
          2. LOG REPLICATION: When `entries` is non-empty, new entries are
             appended to this node's log.

        Args:
            leader_id:       Node ID of the leader sending this RPC.
            term:            Leader's current term.
            prev_log_index:  Index of the entry immediately BEFORE the new
                             entries.  Used for consistency checking.
            prev_log_term:   Term of the entry at prev_log_index.
            entries:         List of LogEntry objects to append (may be empty).
            leader_commit:   Leader's commit_index (so we can advance ours).

        Returns:
            True  if the entries were appended successfully.
            False if rejected (stale term or log mismatch).

        RESPONSIBILITY: CONSENSUS
        WHY: This is how the leader's log gets replicated to every follower.
             The consistency check ensures that followers never have gaps or
             disagreements in their logs.
        """
        # Guard: inactive nodes do not respond
        if not self.active:
            return False

        # Rule 1: Reject if sender's term is stale
        if term < self.current_term:
            return False

        # Update term if sender has a newer term
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None

        #  Recognize the sender as the legitimate leader
        # WHY: Any valid AppendEntries with term >= ours means the sender
        #      is the rightful leader.  We step down if we were a candidate.
        self.state = NodeState.FOLLOWER
        self.leader_id = leader_id

        # Rule 2: Consistency check on previous entry
        # WHY: To ensure logs are identical, we verify that the entry at
        #      prev_log_index has the expected term.  If not, the follower's
        #      log has diverged and the leader must backtrack.
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return False  # We don't have the previous entry
            if self.log[prev_log_index].term != prev_log_term:
                # Remove the conflicting entry and everything after it
                self.log = self.log[:prev_log_index]
                return False

        #  Rule 3: Append new entries (skip duplicates)
        for i, entry in enumerate(entries):
            insert_index = prev_log_index + 1 + i
            if insert_index < len(self.log):
                if self.log[insert_index].term != entry.term:
                    # Conflict: remove this entry and everything after
                    self.log = self.log[:insert_index]
                    self.log.append(entry)
                # Else: entry already exists and matches — skip
            else:
                self.log.append(entry)

        # Rule 4: Advance commit index
        # WHY: The leader tells us its commit_index.  We can safely advance
        #      our own commit_index up to that value (but not past our log).
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            self._apply_committed_entries()

        return True

    # COMMIT LOGIC

    def commit_entries(self) -> None:
        """
        Advance the leader's commit_index based on majority replication.

        An entry at index N is committed when a MAJORITY of nodes have it
        in their log (match_index[peer] >= N for a majority) AND the entry
        is from the current term.

        After advancing commit_index, apply newly committed entries to the
        replication state machine.

        RESPONSIBILITY: CONSENSUS
        WHY: Committing only after majority ensures that a committed entry
             will survive any single node failure — any future leader must
             have it in its log (by the election restriction).
        """
        if self.state != NodeState.LEADER:
            return

        total_nodes = len(self.peers) + 1
        majority_needed = (total_nodes // 2) + 1

        # Check each uncommitted entry starting from the end of the log
        # (optimization: start from the highest possible new commit point).
        for n in range(len(self.log) - 1, self.commit_index, -1):
            # Only commit entries from the CURRENT term.
            # WHY: Raft never directly commits entries from previous terms.
            #      They get committed indirectly when a current-term entry
            #      after them is committed (§5.4.2 of the Raft paper).
            if self.log[n].term != self.current_term:
                continue

            # Count how many nodes have this entry
            replicated_count = 1  # Leader has it
            for peer in self.peers:
                if self.match_index.get(peer.node_id, -1) >= n:
                    replicated_count += 1

            if replicated_count >= majority_needed:
                self.commit_index = n
                self._apply_committed_entries()
                break  # We found the highest committed index

    def _apply_committed_entries(self) -> None:
        """
        Apply all committed but not-yet-applied entries to the replication
        state machine.

        This is where CONSENSUS hands off to REPLICATION.

        For each newly committed entry, we call:
            replication.apply_committed_entry(entry_dict)

        RESPONSIBILITY:
            CONSENSUS  → decides WHEN an entry is committed.
            REPLICATION → stores the committed entry for retrieval.

        WHY: The replication module is the persistent store.  Once Raft has
             achieved consensus on an entry (majority agreement), it is safe
             to apply it to the state machine.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]

            # ===== REPLICATION RESPONSIBILITY =====
            # Hand off the committed entry to the replication module.
            if self.replication is not None:
                self.replication.apply_committed_entry(entry.to_dict())

