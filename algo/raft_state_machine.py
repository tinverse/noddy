"""The Raft State Machine"""

import logging
import math
import os
import threading
import time

from infra.state_machine import State, StateMachine

from . import config
from .controller import RaftController
from .message import EventId, HeartbeatTimeout, ElectionTimeout


class RaftCallbackTimer:
    """Puts an event in the event queue periodically"""
    def __init__(self, timeout_sec=1,
                 cb=None):  # default 1s timer unless overridden_sec
        self.done = threading.Event()
        self.timeout_sec = timeout_sec
        self.cb = cb
        self._reset = False

    def start(self):
        """Start the timer thread"""
        self.done.clear()
        threading.Thread(target=self._run_timer).start()

    def _run_timer(self):
        """Callback for thread."""
        # logging.debug("Run Timer:%f", self.timeout_sec)
        while not self.done.is_set():
            # logging.debug("Sleeping for:%f", self.timeout_sec)
            time.sleep(self.timeout_sec)
            if not self._reset:  # if reset == False
                if self.cb:
                    self.cb()
                self._reset = False
        # logging.debug("Stopping:%f", self.timeout_sec)

    def stop(self):
        self._reset = True
        self.done.set()

    def reset(self):
        self._reset = True


class RaftEventTimer:
    """Puts an event in the event queue periodically"""
    def __init__(self, event, event_queue,
                 timeout_sec=1):  # default 1s timer unless overridden_sec
        self.done = threading.Event()
        self.event = event
        self.event_queue = event_queue
        self.timeout_sec = timeout_sec
        self._reset = False

    def start(self):
        """Start the timer thread"""
        self.done.clear()
        threading.Thread(target=self._run_timer).start()

    def _run_timer(self):
        """Callback for thread."""
        # logging.debug("Run Timer:%f", self.timeout_sec)
        while not self.done.is_set():
            # logging.debug("Sleeping for:%f", self.timeout_sec)
            time.sleep(self.timeout_sec)
            if not self._reset:  # if reset == False
                # logging.debug("Adding event to queue: %s", self.event)
                self.event_queue.put(self.event)
            self._reset = False

        logging.debug("Stopping:%f", self.timeout_sec)

    def stop(self):
        self._reset = True
        self.done.set()

    def reset(self):
        self._reset = True


class HeartbeatTimer(RaftEventTimer):
    def __init__(self, event_queue):
        event = HeartbeatTimeout()
        super().__init__(event, event_queue,
                         config.HEARTBEAT_TIMEOUT_MS / 1000)


class ElectionTimer(RaftEventTimer):
    """Generate timeout event at expiration"""
    def __init__(self, event_queue):
        event = ElectionTimeout()
        super().__init__(event, event_queue)

    def start(self):
        """Set randomized timeout and start the election timer"""
        def _calc_timeout_ms():
            """Calculate random election timeout"""
            timeout = int(os.urandom(4).hex(), 16) & (0x7FFFFFFF)
            return (timeout / (math.pow(2, 31) - 1)) * (
                config.ELECTION_TIMEOUT_MAX_MS - config.ELECTION_TIMEOUT_MIN_MS
            ) + config.ELECTION_TIMEOUT_MIN_MS

        self.timeout_sec = _calc_timeout_ms() / 1000
        # logging.debug("Election Timeout:%f", self.timeout_sec)
        threading.Thread(target=super()._run_timer).start()


class CandidateState(State):
    """"Candidate State"""
    def __init__(self, parent):
        super().__init__("CANDIDATE", parent)
        self.election_timer = ElectionTimer(parent.event_queue)

    def enter(self):
        logging.info("%d: Becoming Candidate", self.parent.server_id)
        self.election_timer.start()

    def exit(self):
        logging.debug("%d: Exiting Candidate", self.parent.server_id)
        self.election_timer.stop()


class FollowerState(State):
    """"Follower State"""
    def __init__(self, parent):
        super().__init__("FOLLOWER", parent)
        self.election_timer = ElectionTimer(parent.event_queue)

    def enter(self):
        """Start the election Timer"""
        self.election_timer.start()
        logging.debug("%d: Becoming Follower", self.parent.server_id)

    def exit(self):
        """Stop the election Timer"""
        logging.debug("%d: Exiting Follower", self.parent.server_id)
        self.election_timer.stop()


class LeaderState(State):
    """"Leader State """
    def __init__(self, parent):
        super().__init__("LEADER", parent)
        self.timer = HeartbeatTimer(parent.event_queue)

    def enter(self):
        logging.debug("%d: Becoming Leader", self.parent.server_id)
        self.timer.start()

    def exit(self):
        logging.debug("%d: Exiting Leader", self.parent.server_id)
        self.timer.stop()


class ShutdownState(State):
    """"Shutdown State """
    def __init__(self, parent):
        super().__init__("SHUTDOWN", parent)

    def enter(self):
        logging.debug("Shutdown...")

    def exit(self):
        pass


class RaftStateMachine(StateMachine):
    """Processes messges from both the cluster and clients"""
    def __init__(self):

        # create the starting state
        follower = FollowerState(self)
        shutdown = ShutdownState(self)
        leader = LeaderState(self)
        candidate = CandidateState(self)
        # Then initialize the base class
        super().__init__("RaftStateMachine", follower, shutdown)

        # Event and Message Queue
        # States

        # Events
        heartbeat_event = EventId.HEARTBEAT_TIMEOUT
        election_timeout_event = EventId.ELECTION_TIMEOUT
        ask_vote_request_event = EventId.ASK_VOTE
        ask_vote_response_event = EventId.ASK_VOTE_RESPONSE
        append_entries_request_event = EventId.APPEND_ENTRIES_REQUEST
        append_entries_response_event = EventId.APPEND_ENTRIES_RESPONSE
        shutdown_request_event = EventId.SHUTDOWN_REQUEST

        table = self.table

        # Leader Election
        table.add(follower, election_timeout_event, candidate,
                  self.start_election)
        table.add(candidate, election_timeout_event, candidate,
                  self.start_election)

        table.add_internal(leader, ask_vote_response_event, None)
        table.add_internal(follower, ask_vote_response_event, None)
        table.add_internal(follower, append_entries_response_event, None)

        table.add(candidate, ask_vote_request_event, follower,\
                       self.handle_ask_vote_request, self.handle_ask_vote_request_guard)

        table.add_internal(candidate, ask_vote_response_event,
                           self.handle_ask_vote_response)

        # Handle candidate asking for a vote
        table.add(follower, ask_vote_request_event, follower,\
                                self.handle_ask_vote_request, self.handle_ask_vote_request_guard)
        table.add(leader, ask_vote_request_event, follower,\
                       self.handle_ask_vote_request, self.handle_ask_vote_request_guard)

        # Log Replication
        table.add(follower, append_entries_request_event, follower,\
                       self.handle_append_entries_request, self.handle_append_entries_request_guard)
        table.add(candidate, append_entries_request_event, follower,\
                       self.handle_append_entries_request, self.handle_append_entries_request_guard)
        table.add(leader, append_entries_request_event, follower,
                  self.handle_append_entries_request,
                  self.handle_append_entries_request_guard)

        # Handle heartbeat request - basically an empty append entries
        table.add_internal(leader, heartbeat_event,
                           self.handle_heartbeat_request)

        table.add_internal(leader, append_entries_response_event,
                           self.handle_append_entries_response)

        # Shutdown
        table.add(follower, shutdown_request_event, shutdown,
                  self.handle_shutdown)
        table.add(candidate, shutdown_request_event, shutdown,
                  self.handle_shutdown)
        table.add(leader, shutdown_request_event, shutdown,
                  self.handle_shutdown)

        # Create the controller
        self.algo = RaftController(self.server_id)

        #hold a ref to the follower and leader for immediate transitions
        self.follower = follower
        self.leader = leader

        # Lastly, enter the start state
        self.current_state.enter()

    def become_follower(self, *_):
        logging.info("%d: Handle Becoming Follower", self.server_id)
        self.algo.voted_for = None
        self.algo.votes_received = 0

    def become_leader(self, *_):
        logging.info("%d: Handle Becoming Leader", self.server_id)
        for key in self.algo.next_index:
            self.algo.next_index[key] = len(self.algo.log)
            if len(self.algo.log) > 0:
                self.algo.match_index[key] = 0
            else:
                self.algo.match_index[key] = -1
        self.handle_heartbeat_request()

    def handle_append_entries_request_guard(self, msg):
        response = self.algo.process_append_entries_request_guard(
            term=msg.term,
            leader_id=msg.leader_id,
            previous_log_index=msg.previous_log_index,
            previous_log_term=msg.previous_log_term)
        if not response.result:
            self.send(response, response.leader_id)
        # Guard has to return a T/F value
        return response.result

    def handle_append_entries_request(self, msg):
        """Process the request and respond to it"""
        response = self.algo.process_append_entries_request(
            msg.term, msg.leader_id, msg.previous_log_index,
            msg.previous_log_term, msg.entries, msg.leader_commit)
        #if ret_val and self.current_state.name != 'LEADER': # clean this up
        #    self.current_state.election_timer.reset = True
        self.send(response, response.leader_id)

    def handle_append_entries_response(self, msg):
        ret = self.algo.process_append_entries_response(
            msg.result, msg.server_id, msg.match_index, msg.term)
        if ret and ret.msg_id == EventId.BECOME_FOLLOWER:
            self.do_immedite(self.follower, self.become_follower)

    def handle_heartbeat_request(self, *_):
        """Compose the heartbeat request and send to everyone"""
        for server_id in config.CLUSTER_SERVERS:
            if server_id != self.server_id:
                response = self.algo.create_append_entries_request(server_id)
                # send the response in a separate thread for each server in the cluster
                threading.Thread(target=self.send,
                                 args=(response, server_id)).start()

    def handle_ask_vote_request_guard(self, msg):
        response = self.algo.process_ask_vote_request_guard(
            msg.term, msg.candidate_id, msg.last_log_index, msg.last_log_term)
        if not response.result:
            self.send(response, response.candidate_id)
        # Guard has to return a T/F value
        return response.result

    def handle_ask_vote_request(self, msg):
        response = self.algo.process_ask_vote_request(msg.term,
                                                      msg.candidate_id,
                                                      msg.last_log_index,
                                                      msg.last_log_term)
        if response.result and self.current_state.name != 'LEADER':
            self.current_state.election_timer.reset()
        self.send(response, response.candidate_id)

    def handle_ask_vote_response(self, msg):
        ret = self.algo.process_ask_vote_response(msg.result, msg.current_term)
        if ret and ret.msg_id == EventId.BECOME_FOLLOWER:
            logging.debug("%d: Converting to follower from %s", self.server_id,
                          self.current_state.name)
            self.do_immedite(self.follower, self.become_follower)
        elif ret and ret.msg_id == EventId.BECOME_LEADER:
            self.do_immedite(self.leader, self.become_leader)

    def start_election(self, msg):
        """Start an election - on election_timeout_event"""
        logging.info("%d: Starting Election on %s timeout", self.server_id,
                     self.current_state.name)
        msg = self.algo.create_ask_vote_request()
        self.send_all_servers(msg)

    def handle_elected_event(self, msg):
        """So you are the leader"""
        # initialize next_index and match_index
        for key in self.algo.next_index:
            self.algo.next_index[key] = len(self.algo.log)
            if len(self.algo.log) > 0:
                self.algo.match_index[key] = 0
            else:
                self.algo.match_index[key] = -1
        # send empty entries to broadcast leadership
        self.event_queue.put(HeartbeatTimeout())

    def handle_shutdown(self, msg):
        pass
