"""The user/client facing raft classes"""
import pickle
import logging
import queue
import threading
import time

from net.tcp_server import TcpServer
from net.tcp_client import TcpClient
from net.conn_handlers import ThreadedHandler

from .config import CLUSTER_SERVERS, CLIENT_FACING_SERVERS
from .raft_state_machine import RaftStateMachine
from .message import ShutdownRequest


class ThreadedMessageProducer(TcpServer, ThreadedHandler):
    """Produces messages from the Raft cluster - Puts them in the event queue"""
    def __init__(self, server_ip, server_port, event_queue, server_id):
        #default - backlog = 5
        super().__init__(server_ip, server_port, backlog=5)
        self.event_queue = event_queue
        self.server_id = server_id
        self._close = False

    def respond(self, conn):
        """Invoked by the threaded handler"""
        with conn:
            try:
                while True:
                    msg = TcpServer.recv(self, conn)
                    msg = pickle.loads(msg)
                    if not msg:
                        break
                    # logging.info("%d: Received msg:%s", self.server_id, msg.msg_id)
                    self.event_queue.put(msg)
            except ConnectionError:
                if self.shutting_down:
                    pass
                else:
                    logging.error("ConnectionError")

    def close(self):
        TcpServer.close(self)


class RaftServer:
    """Raft Server. Handles Tcp communication from clients and other servers in the cluster"""
    def __init__(self, server_id):
        self.server_id = server_id
        self.event_queue = queue.Queue()
        super().__init__()
        server_ip, server_port = CLUSTER_SERVERS[self.server_id]

        self.cluster_message_producer = ThreadedMessageProducer(
            server_ip, server_port, self.event_queue, server_id)

        client_facing_server_ip, client_facing_server_port = CLIENT_FACING_SERVERS[
            self.server_id]
        self.client_message_producer = ThreadedMessageProducer(
            client_facing_server_ip, client_facing_server_port,
            self.event_queue, server_id)
        # consume messages
        # dict of TcpClient objects
        self._socks = {
            client: None
            for client in CLUSTER_SERVERS if client != server_ip
        }
        self.cluster_server_thread = None
        self.cluster_server_thread = None
        logging.debug("Created Raft Server: %d", self.server_id)

    def send(self, msg, dest):
        if not self._socks[dest]:
            try:
                client_ip, client_port = CLUSTER_SERVERS[dest]
                self._socks[dest] = TcpClient(client_ip, client_port)
                logging.debug("%d: Connected to %d", self.server_id, dest)
            except IOError:
                logging.debug("%d: Connection to %d failed", self.server_id,
                              dest)
        if self._socks[dest]:
            try:
                logging.debug("%d: Sending %s", self.server_id, msg.msg_id)
                msg = pickle.dumps(msg)  # this encodes msg to bytes
                self._socks[dest].send(msg)
            except IOError:
                if not self.shutdown_event.is_set():
                    logging.error("%d: Send to %d failed", self.server_id,
                                  dest)
                    self._socks[dest] = None

    def send_all_servers(self, msg):
        """send message to all servers"""
        for i, dest in enumerate(self._socks):
            if i != self.server_id:
                threading.Thread(target=self.send, args=(msg, dest)).start()

    def initialize_connections(self):
        for server_id, tcp_client in enumerate(self._socks):
            if not self._socks[server_id] and (server_id != self.server_id):
                while True:
                    try:
                        client_ip, client_port = CLUSTER_SERVERS[server_id]
                        self._socks[server_id] = TcpClient(
                            client_ip, client_port)
                        logging.debug("%d: Connected to %d", self.server_id,
                                      server_id)
                        break
                    except IOError:
                        logging.error("%d: waiting for server %d",
                                      self.server_id, server_id)
                    time.sleep(1)

    def recv(self, client_id):
        msg = self._socks[client_id].recv()
        return pickle.loads(msg)


class Raft(RaftServer, RaftStateMachine):
    def __init__(self, server_id):
        super().__init__(server_id)
        # Tcp Server communicating with other raft servers in the cluster
        self.cluster_thread = threading.Thread(
            target=self.cluster_message_producer.run)
        # cfs - client facing server
        self.cfs_thread = threading.Thread(
            target=self.client_message_producer.run)
        # sm - state machine
        self.sm_thread = threading.Thread(target=self.execute)
        self.shutdown_event = threading.Event()

    def run(self):
        """Call the TcpServer run in a separate thread"""
        # Start the servers
        self.cluster_thread.start()

        # check if *all* other client sockets are up
        self.initialize_connections()

        # Start the clients
        self.cfs_thread.start()

        # Start The state machine
        self.sm_thread.start()

        try:
            self.shutdown_event.wait()
        except KeyboardInterrupt:
            pass

        self.sm_thread.join()
        self.cfs_thread.join()
        self.cluster_thread.join()

    def print_status(self):
        logging.info("%d: %s %s", self.server_id, self.current_state.name,
                     self.algo.log)

    def shutdown(self):
        self.print_status()
        self.shutdown_event.set()
        self.event_queue.put(ShutdownRequest())
        time.sleep(0.01)
        while not self.event_queue.empty():
            evt = self.event_queue.get()
            logging.debug("Popped off event: %s", evt.msg_id)
        self.client_message_producer.close()
        self.cluster_message_producer.close()
