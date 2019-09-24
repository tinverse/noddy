"""Base State Machine class"""
import logging


class Event:  # pylint: disable=too-few-public-methods
    """Events are mostly messages"""
    def __init__(self, evt_id, msg=None):
        self.evt_id = evt_id
        self.msg = msg

    def __eq__(self, other):
        return self.evt_id == other.evt_id


class State:
    """Base State"""
    def __init__(self, name, parent):
        self.name = name
        self.parent = parent

    def execute(self):
        pass


class Transition:  # pylint: disable=too-few-public-methods
    """Helper class for performing transition"""
    def __init__(
            self,
            from_state,  # pylint: disable=too-many-arguments
            on_event,
            to_state,
            action=None,
            guard=None):
        self.from_state = from_state
        self.on_event = on_event
        self.to_state = to_state
        self.action = action
        self.guard = guard

    def do_transition(self, msg):
        if self.guard and not self.guard(msg):
            return False
        self.from_state.exit()
        if self.action:
            self.action(msg)
        self.to_state.enter()
        return True


class InternalTransition(Transition):  # pylint: disable=too-few-public-methods
    """InternalTransitions skip the exit and entry actions and stay in the same state"""
    """Works just like a transition except, does not perform the entry and exit
    actions. Remains in the same state"""

    # pylint: disable=too-many-arguments
    def __init__(self, from_state, on_event, action=None, guard=None):
        super().__init__(from_state, on_event, from_state, action)

    def do_transition(self, msg):
        if self.guard and not self.guard(msg):
            return False
        if self.action:
            self.action(msg)
        # Note: You have to return false here. In an internal transition,
        # we are not transitioning to a new state.
        return False


class TransitionTable:
    """Transition Table - Dict of transitions"""
    def __init__(self):
        self._table = {}

    def add(self, from_state, on_event, to_state, action, guard=None):
        """Create a state, event pair as a key and insert a Transition as value"""
        pair = (from_state, on_event)
        self._table[pair] = Transition(from_state, on_event, to_state, action,
                                       guard)

    def add_internal(self, from_state, on_event, action, guard=None):
        """Create a state, event pair as a key and insert an InternalTransition as value"""
        pair = (from_state, on_event)
        self._table[pair] = InternalTransition(from_state, on_event, action,
                                               guard)

    def next(self, state, event):
        transition = None
        try:
            pair = (state, event)
            transition = self._table[pair]
        except KeyError:
            logging.error("%d: No transition from %s on %s.",
                          state.parent.server_id, state.name, event)
        return transition

    def print_table(self):
        """Print the transition table"""
        for pair, transition in self._table.items():
            logging.debug("(%s, \t%s) \t\t=> (%s)", pair[0].name, pair[1],
                          transition.to_state.name)


class StateMachine(State):
    def __init__(self, name, start_state, stop_state):
        super().__init__(name, None)
        self.table = TransitionTable()
        self.start_state = start_state
        self.current_state = self.start_state
        self.stop_state = stop_state

    def execute(self):
        while True:
            prev_state_name = self.current_state.name
            evt = self.event_queue.get()
            self.step(evt)
            if self.current_state.name != prev_state_name:
                logging.debug("%d: State changed from %s to %s",
                              self.current_state.parent.server_id,
                              prev_state_name, self.current_state.name)
            if self.current_state is self.stop_state:
                break

    def step(self, msg):
        logging.debug("%d: Received Message - %s", self.server_id, msg.msg_id)
        transition = self.table.next(self.current_state, msg.msg_id)
        if transition and transition.do_transition(msg):
            self.current_state = transition.to_state

    def do_immedite(self, to_state, action=None):
        self.current_state.exit()
        if action:
            action()
        to_state.enter()
        self.current_state = to_state
