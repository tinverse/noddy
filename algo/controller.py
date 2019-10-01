"""The Raft Algorithm class. State variables, RPCs and Rules for Servers other
than StateMachine stuff"""
from collections import namedtuple
import logging

from . import config
from .message import AskVoteRequest, AskVoteResponse,\
    AppendEntriesRequest, AppendEntriesResponse, BecomeFollower, BecomeLeader

LogEntry = namedtuple("LogEntry", ["term", "value"])


class RaftLog(list):
    """The Raft Log"""
    def __init__(self):
        super().__init__()

    def __setitem__(self, index, value):
        if isinstance(value, list):
            value = LogEntry(term=value[0], value=value[1])
        super().__setitem__(index, value)

class AlgoState:
    """Stores Algorithm state"""
    def __init__(self):
        # persistent state
        self.current_term = 0
        self.log = RaftLog()
        self.voted_for = None

        # volatile state
        self.commit_index = -1
        self.last_applied = -1

        # leader state
        self.next_index = {x: 0 for x in config.CLUSTER_SERVERS}
        self.match_index = {x: -1 for x in config.CLUSTER_SERVERS}

        #Other
        self.leader_id = -1
        self.votes_received = 0

        super().__init__()


class LeaderElection:
    """Implements the LeaderElection algorithm"""
    def __init__(self):
        super().__init__()

    def process_ask_vote_request_guard(self, term, candidate_id,
                                       last_log_index, last_log_term):
        """There will be no state transition if guard returns false"""
        ret = True
        if term < self.current_term:
            ret = False
            logging.info("%d with term=%d rejected %d's request with term=%d",
                         self.server_id, self.current_term, candidate_id, term)
        elif term == self.current_term and self.voted_for:
            ret = False
            logging.info(
                "%d with term=%d rejected %d's request with term=%d. Already voted for %d",
                self.server_id, self.current_term, candidate_id, term,
                self.voted_for)
        elif term == self.current_term and len(self.log) - 1 == last_log_index:
            ret = False
            logging.info(
                "%d with term=%d rejected %d's request with term=%d. log_index mismatch",
                self.server_id, self.current_term, candidate_id, term)
        return self.create_ask_vote_response(ret, candidate_id)

    def process_ask_vote_request(self, term, candidate_id, last_log_index,
                                 last_log_term):
        """Request Vote RPC"""
        self.current_term = term
        #if (self.voted_for is None or \
        #    self.voted_for == candidate_id) and \

        self.voted_for = candidate_id
        #self.convert_to_candidate = False
        return self.create_ask_vote_response(True, candidate_id)

    def process_ask_vote_response(self, result, current_term):
        if result:
            self.votes_received += 1
            if self.votes_received > len(config.CLUSTER_SERVERS) / 2:
                return BecomeLeader()
        else:
            if current_term > self.current_term:
                self.current_term = current_term
                return BecomeFollower()
                # discovered server with higher term transition to follower

    def create_ask_vote_request(self):
        """Increments the term every time a request is created"""
        # term, candidate_id, last_log_index, last_log_term)
        self.current_term += 1
        self.votes_received = 1
        self.voted_for = self.server_id
        last_log_index = len(self.log) - 1
        if last_log_index == -1:
            last_log_term = 0
        else:
            last_log_term = self.log[-1].term

        logging.info("%d: Starting Election. Term: %d", self.server_id,
                     self.current_term)
        return AskVoteRequest(term=self.current_term,
                              candidate_id=self.server_id,
                              last_log_index=last_log_index,
                              last_log_term=last_log_term)

    def create_ask_vote_response(self, ret, candidate_id):
        """Composes the response message"""
        return AskVoteResponse(ret, candidate_id, self.server_id,
                               self.current_term)


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

        msg = AppendEntriesRequest(term=self.current_term,
                                   previous_log_index=prev_log_index,
                                   previous_log_term=prev_log_term,
                                   leader_id=self.server_id,
                                   entries=self.log[next_index:],
                                   leader_commit=self.commit_index)
        return msg

    def process_append_entries_request_guard(self, term, leader_id,
                                             previous_log_index,
                                             previous_log_term):
        ret = True
        try:
            if (term < self.current_term) or \
             (previous_log_index >= 0 and \
              self.log[previous_log_index].term != previous_log_term):
                ret = False
        except IndexError:
            ret = False
        return self.create_append_entries_response(ret, leader_id)

    # incorporate leader_id to redirect clients
    def process_append_entries_request(self, term, leader_id,
                                       previous_log_index, previous_log_term,
                                       entries, leader_commit):
        # if we get this far, we've established that the request has come from
        # who we think is the leader
        current_index = previous_log_index + 1
        # Note: This is where we acknowledge the new leader
        self.leader_id = leader_id

        if entries:  # not a heartbeat

            # Append
            self.log[current_index:] = [LogEntry(e[0], e[1]) for e in entries]

            if leader_commit > self.commit_index:
                index_last_new_entry = len(self.log) - 1
                self.commit_index = min(leader_commit, index_last_new_entry)
        #self.convert_to_candidate = False
        self.voted_for = None
        return self.create_append_entries_response(True, leader_id)

    def process_append_entries_response(self, result, server_id, match_index,
                                        term):
        if not result:
            if self.current_term < term:
                self.current_term = term
                return BecomeFollower()
            self.next_index[server_id] -= 1
        else:
            self.match_index[server_id] = match_index
            match_indices = [v for k, v in self.match_index.items()]
            match_indices.sort()
            self.commit_index = match_indices[len(match_indices) // 2]

    def create_append_entries_response(self, res, leader_id):
        """This is created in response to an append entries request"""
        return AppendEntriesResponse(result=res,
                                     leader_id=leader_id,
                                     server_id=self.server_id,
                                     match_index=len(self.log),
                                     term=self.current_term)


class RaftController(LogReplication, LeaderElection, AlgoState):
    """Mixes in above classes"""
    def __init__(self, server_id):
        self.server_id = server_id
        super().__init__()
