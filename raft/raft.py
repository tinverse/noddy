"""The user/client facing raft classes"""
import json
import logging
import threading
from queue import Queue

from .raft_state_machine import RaftStateMachine
from net.tcp_server import TcpServer
from net.tcp_client import TcpClient
from net.conn_handlers import ThreadedHandler
from .config import CLUSTER_SERVERS, CLIENT_FACING_SERVERS


class ThreadedTcpServer(TcpServer, ThreadedHandler):
    """TcpServer that 'accepts' connections and handles new connections in a
    thread"""

class ThreadedMessageProducer(TcpServer, ThreadedHandler):
    """Produces messages from the Raft cluster - Puts them in the event queue"""
    def __init__(self, server_ip, server_port, backlog, event_queue):
        super().__init__(server_ip, server_port, backlog)
        self.event_queue = event_queue

    def respond(self, conn):
        """Invoked by the threaded handler"""
        with conn:
            msg = TcpServer.recv(self, conn)
            self.event_queue.put(json.loads(msg))
            logging.debug("Received msg:%s", msg)


class RaftServer:
    """Raft server is a TcpServer. It runs the server in a separate Thread"""
    def __init__(self, server_id):
        self.server_id = server_id
        self.event_queue = Queue()
        super().__init__()
        server_ip, server_port = CLUSTER_SERVERS[self.server_id]
        #default - backlog
        backlog = 5
        self.cluster_message_producer = ThreadedMessageProducer(server_ip,
                                                                server_port,
                                                                backlog,
                                                                self.event_queue)

        client_facing_server_ip, client_facing_server_port = CLIENT_FACING_SERVERS[self.server_id]
        self.client_message_producer = ThreadedMessageProducer(client_facing_server_ip,
                                                               client_facing_server_port,
                                                               backlog,
                                                               self.event_queue)
        # consume messages
        # dict of TcpClient objects
        self._socks = {client : None for client in CLUSTER_SERVERS if client != server_ip}
        self.cluster_server_thread = None
        self.cluster_server_thread = None
        logging.debug("Created Raft Server: %d", self.server_id)

    def send(self, dest, msg):
        if not self._socks[dest]:
            try:
                client_ip, client_port = CLUSTER_SERVERS[dest]
                self._socks[dest] = TcpClient(client_ip, client_port)
                logging.info("Connected to %d", dest)
            except IOError:
                logging.error("Connection to %d failed", dest)
        if self._socks[dest]:
            try:
               msg = json.dumps(msg.__dict__)
               self._socks[dest].send(msg)
            except IOError:
                logging.error("Send to %d failed", dest)

    def send_all_servers(self, msg):
        """Iteratively send message to all servers"""
        for dest in self._socks:
            self.send(dest, msg)

    def recv(self, client_id):
        return self._socks[client_id].recv()


class Raft(RaftServer, RaftStateMachine):
    def __init__(self, server_id):
        super().__init__(server_id)

    def run(self):
        """Call the TcpServer run in a separate thread"""
        threading.Thread(target=self.cluster_message_producer.run)
        threading.Thread(target=self.client_message_producer.run)
        # Start The state machine
        threading.thread(target=self.execute)



class BasicTask:
    def __init__(self):
        self._queue = Queue()
        super().__init__()

    def respond(self, conn):
        """respond to client connections"""
        with conn:
            msg = TcpServer.recv(self, conn)
            self._queue.put(msg)
            logging.debug("Received msg:%s", msg)

class LogEntry:
    def __init__(self, term, value):
        self.term = term
        self.value = value

    def __eq__(self, other):
        self.term == other.term and \
            self.value == other.value



class BasicRaftThreadedServer(RaftServer, ThreadedHandler, BasicTask):
    def __init__(self, server_id):
        super().__init__(server_id)
        logging.debug("Created BasicRaftThreadedServer")

    def close(self):
        pass

def main():
    import sys
    logging.basicConfig(level=logging.DEBUG)
    server_id = int(sys.argv[1])
    r = BasicRaftThreadedServer(server_id)
    r.run()

if __name__ == "__main__":
    main()
