#!/usr/bin/env python3
"""Create a checkpoint at various intervals"""
import logging
import pathlib
import pickle
import signal

from kvserver import ThreadedKeyValServer

CHECKPOINT_FILE="store.pkl"

class ThreadedKeyValChkptServer(ThreadedKeyValServer): # pylint: disable=too-few-public-methods
    def __init__(self, ip, port, backlog):
        logging.debug("ThreadedKeyValChkptServer  __init__")
        signal.signal(signal.SIGINT, self.checkpt_handler)
        signal.signal(signal.SIGTERM, self.checkpt_handler)

        super().__init__(ip, port, backlog)
        if pathlib.Path(CHECKPOINT_FILE).is_file():
            with open(CHECKPOINT_FILE, "rb") as f:
                self.store = pickle.load(f)
        logging.debug("Created ThreadedKeyValChkptServer")

    def checkpt_handler(self, signum, frame):
        logging.error("Signal: %d", signum)
        with open(CHECKPOINT_FILE, "wb") as f:
            pickle.dump(self.store, f)
        self.close()
        logging.error("saved store")


def main():
    """Main fn"""
    logging.basicConfig(level=logging.DEBUG)
    server = ThreadedKeyValChkptServer('localhost', 30000, 5)
    server.run()

if __name__ == '__main__':
    main()

