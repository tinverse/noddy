from enum import IntEnum


class EventId(IntEnum):
    """Event IDs - Could be message ids as well"""
    # Please list in order of priority. Higher number gets higher priority
    HEARTBEAT_TIMEOUT = 1
    ELECTION_TIMEOUT = 2
    APPEND_ENTRIES_REQUEST = 6
    APPEND_ENTRIES_RESPONSE = 7
    ASK_VOTE = 8
    ASK_VOTE_RESPONSE = 9
    CLIENT_APPEND_REQUEST = 10
    SHUTDOWN_REQUEST = 11
    HEARTBEAT = 12
    BECOME_LEADER = 13
    BECOME_FOLLOWER = 14


class Message:
    def __init__(self, msg_id):
        self.msg_id = msg_id

    def __lt__(self, other):
        return self.msg_id < other.msg_id


class AskVoteRequest(Message):
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        super().__init__(EventId.ASK_VOTE)
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term


class AskVoteResponse(Message):
    def __init__(self, result, candidate_id, server_id, current_term):
        super().__init__(EventId.ASK_VOTE_RESPONSE)
        self.result = result
        self.candidate_id = candidate_id
        self.current_term = current_term
        self.server_id = server_id


class AppendEntriesRequest(Message):
    def __init__(self, term, previous_log_index, previous_log_term, leader_id,
                 entries, leader_commit):
        super().__init__(EventId.APPEND_ENTRIES_REQUEST)
        self.term = term
        self.previous_log_index = previous_log_index
        self.previous_log_term = previous_log_term
        self.leader_id = leader_id
        self.entries = entries
        self.leader_commit = leader_commit


class AppendEntriesResponse(Message):
    def __init__(self, result, leader_id, server_id, match_index, term):
        super().__init__(EventId.APPEND_ENTRIES_RESPONSE)
        self.result = result
        self.leader_id = leader_id
        self.server_id = server_id
        self.match_index = match_index
        self.term = term


class HeartbeatTimeout(Message):
    def __init__(self):
        super().__init__(EventId.HEARTBEAT_TIMEOUT)


class ElectionTimeout(Message):
    def __init__(self):
        super().__init__(EventId.ELECTION_TIMEOUT)


class BecomeLeader(Message):
    def __init__(self):
        super().__init__(EventId.BECOME_LEADER)


class ClientAppendRequest(Message):
    def __init__(self):
        super().__init__(EventId.CLIENT_APPEND_REQUEST)


class ShutdownRequest(Message):
    def __init__(self):
        super().__init__(EventId.SHUTDOWN_REQUEST)


class BecomeFollower(Message):
    def __init__(self):
        super().__init__(EventId.BECOME_FOLLOWER)
