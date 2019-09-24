#!/usr/bin/env python3
"""The Key Val Server"""
import logging

from net.conn_handlers import ThreadedHandler
from net.tcp_server import TcpServer

class _KeyValStore: # pylint: disable=too-few-public-methods
    """Server's job is to function as a key/val store"""
    def __init__(self):
        logging.debug("In KeyValStore __init__")
        # check thread safety
        self.store = {}
        logging.debug("Created KeyValStore")
        super().__init__()

    def respond(self, conn):
        """Keep reading all data from the client. Invoke the process method to
        parse the data and deal with it appropriately"""
        with conn:
            try:
                while True:
                    data = self.recv(conn)
                    if not data:
                        break
                    response = self.process(data)
                    if response:
                        self.send(conn, response)
            except ConnectionError:
                logging.error("ConnectionError")

    def process(self, data):
        """Invoke get, set, delete"""
        response = None
        try:
            _ = data.split()
            cmd = _[0].lower()
            args = _[1:]
            if cmd == "get":
                response = self._get(args[0])
            elif cmd == "set":
                self._set(key=args[0], value=args[1])
            elif cmd == "delete":
                self._delete(args[0])
            else:
                raise ValueError("Invalid command {0}".format(cmd))
        except ValueError as err:
            logging.debug("ValueError: %s", err)
        return response

    def _get(self, value):
        logging.debug("get: %s", value)
        return self.store[value]

    def _set(self, key, value):
        logging.debug("set: %s %s", key, value)
        self.store[key] = value

    def _delete(self, key):
        logging.debug("delete: %s", key)
        del self.store[key]

    def __del__(self):
        logging.error("Calling KVStore del")

class ThreadedKeyValServer(TcpServer, ThreadedHandler, _KeyValStore): # pylint: disable=too-few-public-methods
    """The key val store"""
    def __init__(self, ip, port, backlog):
        super().__init__(ip, port, backlog)

    def close(self):
        """Call destructors in an appropriate order. First shutdown all client
        sockets, join client threads and then the server socket."""
        for thread in self.client_threads:
            thread.join()

def main():
    """Main fn"""
    logging.basicConfig(level=logging.DEBUG)
    server = ThreadedKeyValServer('localhost', 30000, 5)
    server.run()

if __name__ == '__main__':
    main()
