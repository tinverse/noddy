"""The Raft Algorithm class. State variables, RPCs and Rules for Servers other
than StateMachine stuff"""
from collections import namedtuple
import logging

from . import config
from .message import AskVoteRequest, AskVoteResponse,\
    AppendEntriesRequest, AppendEntriesResponse

LogEntry = namedtuple("LogEntry", ["term", "value"])

class RaftLog:
    """The Raft Log"""
    def __init__(self):
        self.log = []

    def __len__(self):
        return len(self.log)

    def __repr__(self):
        return "RaftLog({0})".format(self.log)

    def __getitem__(self, index):
        return self.log[index]

    def __setitem__(self, index, value):
        self.log[index] = value

class AlgoState:
    """Stores Algorithm state"""
    def __init__(self):
        # persistent state
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog()

        # volatile state
        self.commit_index = -1
        self.last_applied = -1

        # leader state
        self.next_index = {x : 0 for x in config.CLUSTER_SERVERS}
        self.match_index = {x : -1 for x in config.CLUSTER_SERVERS}

        #Other
        self.leader_id = -1
        self.votes_received = 0

        super().__init__()

class LeaderElection:
    """Implements the LeaderElection algorithm"""
    def __init__(self):
        super().__init__()

    def process_ask_vote_request_guard(self, term, candidate_id, last_log_index,
                               last_log_term):
        ret = True
        if term < self.current_term or self.voted_for:
            ret = False
        return self.create_ask_vote_response(ret)


    def process_ask_vote_request(self, term, candidate_id, last_log_index, last_log_term):
        """Request Vote RPC"""
        self.current_term = term
        if (self.voted_for is None or \
            self.voted_for == candidate_id) and \
            len(self.log) - 1 == last_log_index:
            self.voted_for = candidate_id
            #self.convert_to_candidate = False
            ret = True
        return self.create_ask_vote_response(ret)

    def process_ask_vote_response(self, result):
        if result:
            self.votes_received += 1
            if self.votes_received > len(config.CLUSTER_SERVERS) / 2:
                return True
        return False

    def create_ask_vote_request(self):
        """Increments the term every time a request is created"""
        # term, candidate_id, last_log_index, last_log_term)
        last_log_index = len(self.log) - 1
        if last_log_index == -1:
            last_log_term = 0
        else:
            last_log_term = self.log[-1].term

        self.current_term += 1
        msg = AskVoteRequest(
            term=self.current_term,
            candidate_id=self.server_id,
            last_log_index=last_log_index,
            last_log_term=last_log_term)
        return msg

    def create_ask_vote_response(self, ret):
        # term, candidate_id, last_log_index, last_log_term)
        msg = AskVoteResponse(
            result=ret,
            server_id=self.server_id)
        return msg

class LogReplication:
    """Implements the Log Replication Algorithm"""
    def __init__(self):
        super().__init__()

    def create_append_entries_request(self, server_id):
        """create request based on leader's knowledge of the follower state"""
        next_index = self.next_index[server_id]
        current_log_index = len(self.log) - 1
        if current_log_index == -1:
            prev_log_index = -1
            prev_log_term = -1
        else:
            prev_log_index = current_log_index - 1
            logging.debug("prev_log_index:%d", prev_log_index)
            prev_log_term = self.log[prev_log_index].term

        msg = AppendEntriesRequest(
            term=self.current_term,
            previous_log_index=prev_log_index,
            previous_log_term=prev_log_term,
            leader_id=self.server_id,
            entries=self.log[next_index:],
            leader_commit=self.commit_index)
        return msg

    def process_append_entries_request_guard(self, term, prev_log_index, prev_log_term):
        ret = True
        try:
            if term < self.current_term:
                ret = False
            elif prev_log_index >= 0 and \
                self.log[prev_log_index].term != prev_log_term:
                ret = False
        except IndexError:
            ret = False
        return self.create_append_entries_response(ret)

    # incorporate leader_id to redirect clients
    def process_append_entries_request(self, term, leader_id, previous_log_index,
                                       previous_log_term, entries, leader_commit):
        # if we get this far, we've established that the request has come from
        # who we think is the leader
        current_index = previous_log_index + 1
        # Note: This is where we acknowledge the new leader
        self.leader_id = leader_id

        if entries: # not a heartbeat

            # Append
            self.log[current_index:] = [LogEntry(e[0], e[1]) for e in entries]

            if leader_commit > self.commit_index:
                index_last_new_entry = len(self.log) - 1
                self.commit_index = min(leader_commit, index_last_new_entry)
        #self.convert_to_candidate = False
        return self.create_append_entries_response(True)

    def process_append_entries_response(self, result, server_id, match_index):
        if not result:
            self.next_index[server_id] -= 1
        else:
            self.match_index[server_id] = match_index
            match_indices = [v for k, v in self.match_index.items()]
            match_indices.sort()
            self.commit_index = match_indices[ len(match_index) // 2 ]

    def create_append_entries_response(self, res):
        """This is created in response to an append entries request"""
        return AppendEntriesResponse(
            result=res,
            server_id=self.server_id,
            match_index=len(self.log)
        )

class RaftController(LogReplication, LeaderElection, AlgoState):
    """Mixes in above classes"""
    def __init__(self, server_id):
        self.server_id = server_id
        super().__init__()
