"""Base State Machine class"""
import logging

class Event: # pylint: disable=too-few-public-methods
    """Events are mostly messages"""
    def __init__(self, evt_id, msg=None):
        self.evt_id = evt_id
        self.msg = msg

    def __eq__(self, other):
        return self.evt_id == other.evt_id


class State:
    """Base State"""
    def __init__(self, name):
        self.name = name

    def execute(self):
        pass


class Transition:# pylint: disable=too-few-public-methods
    """Helper class for performing transition"""
    def __init__(self, from_state, # pylint: disable=too-many-arguments
                 on_event, to_state, action=None, guard=None):
        self.from_state = from_state
        self.on_event = on_event
        self.to_state = to_state
        self.action = action
        self.guard = guard

    def do_transition(self, msg):
        result = True # I did perform the transition
        if self.guard:
            result = self.guard(msg)
        if (not self.guard) or result:
            self.from_state.exit()
            if self.action:
                self.action(msg)
            self.to_state.enter()
        return result


class InternalTransition(Transition): # pylint: disable=too-few-public-methods
    """InternalTransitions skip the exit and entry actions and stay in the same state"""
    """Works just like a transition except, does not perform the entry and exit
    actions. Remains in the same state"""
    def __init__(self, from_state, # pylint: disable=too-many-arguments
                 on_event, to_state, action=None, guard=None):
        super().__init__(from_state, on_event, to_state, action)

    def do_transition(self, msg):
        result = True
        if self.guard:
            result = self.guard(msg)
        if (not self.guard) or result:
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
        self._table["from_state"] = {on_event : Transition(from_state, on_event, to_state, action, guard)}

    def add_internal(self, from_state, on_event, to_state, action, guard=None):
        self._table["from_state"] = {on_event : InternalTransition(from_state, on_event, to_state, action, guard)}

    def next(self, state, event):
        transition = None
        try:
            transition = self._table[state][event]
        except KeyError:
            logging.error("No transition from %s on %s", state, repr(event.evt_id))
        return transition


