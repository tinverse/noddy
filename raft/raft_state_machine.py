"""The Raft State Machine"""

import math
import os
import pickle
import threading
import time

from infra.state_machine import State, TransitionTable

from . import config
from .controller import RaftController
from .message import EventId, HeartbeatTimeout,\
    ElectionTimeout, ReceivedMajority

class RaftTimer:
    """Puts an event in the event queue periodically"""
    def __init__(self, timeout_sec, event, event_queue):
        self.done = threading.Event()
        self.timeout = timeout_sec
        self.event = pickle.dumps(event)
        self.event_queue = event_queue
        self._reset = False

    def start(self):
        """Start the timer thread"""
        threading.Thread(target=self._run_timer, args=(self.timeout,))

    def _run_timer(self, timeout):
        """Callback for thread."""
        while not self.done.is_set():
            time.sleep(timeout)
            if not self._reset: # if reset == False
                self.event_queue.put(self.event)
                self._reset = False
    def stop(self):
        self.done.set()

    def reset(self):
        self._reset = True

class ElectionTimer(RaftTimer):
    """Generate timeout event at expiration"""
    def __init__(self, event_queue):
        event = ElectionTimeout()
        super().__init__(config.HEARTBEAT_TIMEOUT_MS/1000, event, event_queue)

    def start(self):
        """Set randomized timeout and start the election timer"""

        def _calc_timeout():
            """Calculate random election timeout"""
            timeout = int(os.urandom(4).hex(), 16) & (0x7FFFFFFF)
            return (timeout / (math.pow(2, 31) -1)) * (300 - 150) + 150

        timeout = _calc_timeout()
        threading.Thread(target=super()._run_timer, args=(timeout,))

class CandidateState(State):
    """"Candidate State"""
    def __init__(self, event_queue):
        super().__init__("CANDIDATE")
        self.election_timer = ElectionTimer(event_queue)

    def enter(self):
        self.election_timer.start()

    def exit(self):
        self.election_timer.stop()

class FollowerState(State):
    """"Follower State"""
    def __init__(self, event_queue):
        super().__init__("FOLLOWER")
        self.election_timer = ElectionTimer(event_queue)

    def enter(self):
        """Start the election Timer"""
        self.election_timer.start()

    def exit(self):
        """Stop the election Timer"""
        self.election_timer.stop()

class LeaderState(State):
    """"Leader State """
    def __init__(self, event_queue):
        super().__init__("LEADER")
        self.heartbeat_thread = None
        event = HeartbeatTimeout()
        timeout_sec = config.HEARTBEAT_TIMEOUT_MS/1000 # wait for 50ms
        self.timer = RaftTimer(timeout_sec, event, event_queue)

    def enter(self):
        self.timer.start()

    def exit(self):
        self.timer.stop()

class RaftStateMachine:
    """Processes messges from both the cluster and clients"""
    def __init__(self):
        self.algo = RaftController(self.server_id)

        # Event and Message Queue
        # States
        candidate = CandidateState(self.event_queue)
        leader = LeaderState(self.event_queue)
        follower = FollowerState(self.event_queue)


        # Events
        heartbeat_event = EventId.HEARTBEAT_TIMEOUT
        timeout_event = EventId.ELECTION_TIMEOUT
        elected_event = EventId.RECEIVED_MAJORITY
        ask_vote_request_event  = EventId.ASK_VOTE
        ask_vote_response_event  = EventId.ASK_VOTE_RESPONSE
        append_entries_request_event = EventId.APPEND_ENTRIES_REQUEST
        append_entries_response_event = EventId.APPEND_ENTRIES_RESPONSE


        # Transition Table
        self.table = TransitionTable()

        # external transitions
        self.table.add(follower, timeout_event, candidate, self.start_election)
        self.table.add(candidate, timeout_event, candidate, self.start_election)
        self.table.add(candidate, elected_event, leader, self.handle_elected_event)

        # Handle anyone asking for a vote
        self.table.add(candidate, ask_vote_request_event, candidate,\
                       self.handle_ask_vote_request, self.handle_ask_vote_request_guard)
        self.table.add_internal(candidate, ask_vote_response_event, candidate,\
                                self.handle_ask_vote_response)

        self.table.add_internal(follower, ask_vote_request_event, follower,\
                                self.handle_ask_vote_request, self.handle_ask_vote_request_guard)
        self.table.add(leader, ask_vote_request_event, leader,\
                       self.handle_ask_vote_request, self.handle_ask_vote_request_guard)

        # Handle append entries requests
        self.table.add(follower, append_entries_request_event, follower,\
                       self.handle_append_entries_request, self.handle_append_entries_request_guard)
        self.table.add(candidate, append_entries_request_event, follower,\
                       self.handle_append_entries_request, self.handle_append_entries_request_guard)
        self.table.add(leader, append_entries_request_event, follower,
                       self.handle_append_entries_request, self.handle_append_entries_request_guard)

        # Handle heartbeat request - basically an empty append entries
        self.table.add(leader, heartbeat_event, leader, self.handle_heartbeat_request)

        # internal transitions
        self.table.add_internal(leader, append_entries_response_event, leader,\
                                self.handle_append_entries_response)


        self.start_state = follower
        self.current_state = self.start_state

        self.current_state.enter()

    def execute(self):
        while True:
            msg = pickle.loads(self.queue.get())
            transition = self.table.next(self.current_state, msg.msg_id)
            if transition and transition.do_transition(msg):
                self.current_state = transition.to_state


    def handle_append_entries_request_guard(self, msg):
        response = self.algo.process_append_entries_request_guard(
            msg.term,
            msg.prev_log_index,
            msg.prev_log_term
        )
        if not response.result:
            self.send(response, response.server_id)
        # Guard has to return a T/F value
        return response.result

    def handle_append_entries_request(self, msg):
        """Process the request and respond to it"""
        response = self.algo.process_append_entries_request(
            msg.term,
            msg.leader_id,
            msg.previous_log_index,
            msg.previous_log_term,
            msg.entries,
            msg.leader_commit
        )
        #if ret_val and self.current_state.name != 'LEADER': # clean this up
        #    self.current_state.election_timer.reset = True
        self.send(response, response.server_id)

    def handle_heartbeat_request(self, msg):
        """Compose the heartbeat request and send to everyone"""
        for server_id in config.CLUSTER_SERVERS:
            if server_id != self.server_id:
                response = self.algo.create_append_entries_request(msg.server_id)
                # send the response in a separate thread for each server in the cluster
                threading.Thread(target=self.send, args=(response, server_id))

    def handle_ask_vote_request_guard(self, msg):
        response = self.algo.process_ask_vote_request_guard(
            msg.term,
            msg.candidate_id,
            msg.last_log_index,
            msg.last_log_term)
        if not response.result:
            self.send(response, response.server_id)
        # Guard has to return a T/F value
        return response.result

    def handle_ask_vote_request(self, msg):
        response = self.algo.process_ask_vote_request(
            msg.term,
            msg.candidate_id,
            msg.last_log_index,
            msg.last_log_term
        )
        if response.result:
            self.current_state.election_timer.reset = True
        self.send(response, response.candidate_id)

    def handle_ask_vote_response(self, msg):
        ret = self.algo.process_ask_vote_response(msg.result)
        if ret:
            self.event_queue.put(ReceivedMajority())

    def start_election(self, msg):
        """Start an election - on timeout_event"""
        self.algo.votes_received = 1
        msg = self.algo.create_ask_vote_request()
        self.send_all_servers(msg)


    def handle_elected_event(self, msg):
        """So you are the leader"""
        for key in self.algo.next_index:
            self.algo.next_index[key] = len(self.algo.log)
            self.algo.match_index[key] = 0


    def handle_append_entries_response(self, msg):
        self.algo.process_append_entries_response(
            msg.result)
