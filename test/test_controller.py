import pytest

from raft.controller import RaftController, LogEntry

@pytest.fixture
def raft_empty_controller():
    return RaftController(0)

@pytest.fixture
def raft_empty_controller_1():
    return RaftController(1)

def test_create_append_entries_request(raft_empty_controller):
    msg = raft_empty_controller.create_append_entries_request(1)
    assert msg.term == 0, msg.previous_log_index == -1
    assert msg.previous_log_term == -1, msg.entries == []
    assert msg.leader_id == 0, msg.leader_commit == -1

def test_process_ask_vote_request_response(raft_empty_controller, raft_empty_controller_1):
    msg = raft_empty_controller.create_ask_vote_request()
    msg = raft_empty_controller_1.process_ask_vote_request(
        msg.term,
        msg.candidate_id,
        msg.last_log_index,
        msg.last_log_term
    )
    assert msg.result, msg.server_id == 1

    ret = raft_empty_controller.process_ask_vote_response(msg.result)

    assert ret is False
    assert raft_empty_controller.votes_received == 1

@pytest.fixture
def r0_112_122():
    r = RaftController(0)
    r.current_term = 2
    r.prev_log_index = 1
    r.log = [LogEntry(1,1), LogEntry(1, 2), LogEntry(2, 2)]
    r.next_index = {x : len(r.log) for x in range(5)}
    return r

@pytest.fixture
def r1_111_123():
    r = RaftController(0)
    r.current_term = 2
    r.leader_id = 0
    r.log = [LogEntry(1, 1), LogEntry(1, 2), LogEntry(2, 3)]
    return r

@pytest.fixture
def fig_6():
    leader = RaftController(0)
    leader_log = [(1, 3), (1, 1), (1, 9), (2, 2), (3, 0), (3, 7), (3, 5), (3, 4)]
    leader.log = [LogEntry(entry[0], entry[1]) for entry in leader_log]
    leader.current_term = 3
    leader.commit_index = 6
    leader.next_index = {x : len(leader.log) for x in range(5)}

    follower = RaftController(1)
    follower_log = [(1, 3), (1, 1)]
    follower.log = [LogEntry(entry[0], entry[1]) for entry in follower_log]
    follower.leader_id = 2 #set to someone else
    follower.current_term = 1
    return (leader, follower)

def xtest_process_append_entries_request_response(r0_112_122, r1_112_123):
    """Test the following usecaes:
        1. Follower missing entries on the leader
        2. Follower has extra entries not on the leader
        3. Both 1 & 2
    """
    # term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
    msg = r0_112_122.create_append_entries_request(1)
    r1_112_123.process_append_entries_request(
        msg.term,
        msg.leader_id,
        msg.previous_log_index,
        msg.previous_log_term,
        msg.entries,
        msg.leader_commit
    )

    assert r0_112_122.log == r1_112_123.log

def test_process_append_entries_request_response(fig_6):
    leader, follower = fig_6
    msg = leader.create_append_entries_request(1)

    follower.process_append_entries_request(
        msg.term,
        msg.leader_id,
        msg.previous_log_index,
        msg.previous_log_term,
        msg.entries,
        msg.leader_commit
    )

    assert leader.log == follower.log


