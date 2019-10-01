import collections
import logging
import queue

import pytest

from algo.controller import LogEntry
from algo.raft_state_machine import RaftStateMachine, RaftEventTimer
from algo.message import AppendEntriesResponse, AskVoteResponse, EventId, ElectionTimeout, HeartbeatTimeout, ShutdownRequest


class NullQueue:
    """Just eats all messages added to this queue. Returns None immediately for any get op"""
    def __init__(self):
        pass

    def get(self):
        return None

    def put(self, *_):
        pass


@pytest.fixture
def event_queue():
    return queue.Queue()


@pytest.fixture
def null_event_queue():
    return NullQueue()


@pytest.fixture
def raft_timer(event_queue):
    return RaftEventTimer("Hello", event_queue, 0.1)


def test_raft_timer(raft_timer):
    raft_timer.start()
    event_queue = raft_timer.event_queue
    event = event_queue.get()
    assert event == "Hello"
    raft_timer.stop()


class MessageSink:
    def __init__(self, server_id, event_queue):
        self.server_id = server_id
        self.event_queue = event_queue
        # This holds the messages the servers would receive with the networking layer. All tests at this level check if the correct message was sent
        self.recv_queues = [collections.deque() for i in range(5)]
        super().__init__()

    def send(self, msg, server_id):
        self.recv_queues[server_id].append(msg)

    def send_all_servers(self, msg):
        for i in range(5):
            if i != self.server_id:
                self.send(msg, i)


@pytest.fixture
def raft_sm(null_event_queue):
    class RaftTestStateMachine(MessageSink, RaftStateMachine):
        def __init__(self, server_id, event_queue):
            super().__init__(server_id, event_queue)

    # We are creating a StateMachine with a NullQueue instance
    return RaftTestStateMachine(0, null_event_queue)


def test_initialize_sm(raft_sm):
    controller = raft_sm.algo
    assert raft_sm.server_id == 0
    assert raft_sm.current_state.name == 'FOLLOWER'
    raft_sm.step(ElectionTimeout())
    assert raft_sm.current_state.name == 'CANDIDATE'
    assert controller.votes_received == 1

    # check messages sent
    for server_id in range(5):
        if server_id != raft_sm.server_id:
            while raft_sm.recv_queues[server_id]:
                msg = raft_sm.recv_queues[server_id].pop()
                assert msg.msg_id == EventId.ASK_VOTE

    # recv queues is empty now for all servers
    # Let's respond back with a couple of votes to see if '0' becomes leader
    raft_sm.step(
        AskVoteResponse(True, candidate_id=0, server_id=1, current_term=1))
    raft_sm.step(
        AskVoteResponse(True, candidate_id=0, server_id=2, current_term=1))

    assert controller.votes_received == 3

    assert raft_sm.current_state.name == 'LEADER'
    assert controller.current_term == 1

    # Should votes received still be 3 or reset to 1 or 0?
    assert controller.votes_received == 3

    raft_sm.step(HeartbeatTimeout())
    for server_id in range(5):
        if server_id != raft_sm.server_id:
            while raft_sm.recv_queues[server_id]:
                msg = raft_sm.recv_queues[server_id].pop()
                assert msg.msg_id == EventId.APPEND_ENTRIES_REQUEST
                assert msg.entries == []

    # Handle response from say server 3
    raft_sm.step(
        AppendEntriesResponse(
            result=True,
            leader_id=0,
            server_id=3,
            # match_index = -1 for server 3 as len(log) = 0 now
            # Basically, nothing matches
            match_index=-1,
            term=raft_sm.algo.current_term))
    assert controller.match_index[3] == -1
    assert controller.commit_index == -1
    leader_log = [(1, 3), (1, 1), (1, 9), (2, 2), (3, 0), (3, 7), (3, 5),
                  (3, 4)]
    # Treat this as a client append request
    controller.log = [LogEntry(entry[0], entry[1]) for entry in leader_log]

    raft_sm.step(HeartbeatTimeout())
    for server_id in range(5):
        if server_id != raft_sm.server_id:
            while raft_sm.recv_queues[server_id]:
                msg = raft_sm.recv_queues[server_id].pop()
                assert msg.msg_id == EventId.APPEND_ENTRIES_REQUEST

    raft_sm.step(ShutdownRequest())
