"""Various handlers"""
import logging
import threading


class SequentialHandler:  # pylint: disable=too-few-public-methods
    """Threaded Handler - Every conn connection is processed in a separate
    thread"""
    def __init__(self):
        logging.debug("In SequentialHandler __init__")
        super().__init__()
        logging.debug("Created SequentialHandler")

    def handle(self, conn, addr):
        """Create and execute in current Thread"""
        logging.debug("Connection from: %s", addr)
        # pylint: disable=no-member
        self.respond(conn)


class ThreadedHandler:  # pylint: disable=too-few-public-methods
    """Threaded Handler - Every conn connection is processed in a separate
    thread"""
    def __init__(self):
        logging.debug("In ThreadedHandler __init__")
        super().__init__()
        logging.debug("Created ThreadedHandler")

    def handle(self, conn, addr):
        """Create and execute in separate thread"""
        logging.debug("Connection from: %s", addr)
        # pylint: disable=no-member
        threading.Thread(target=self.respond, args=(conn, )).start()
