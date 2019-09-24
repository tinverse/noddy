import logging
import socket

from net import socket_ops

BUF_SIZE = 512
class TcpClient:
    """Creates a client socket on an ip:port."""

    def __init__(self, ip, port):
        """ Create a socket and listen """
        super().__init__()
        self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Warning: Understand implications of REUSEADDR
        # self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.conn.connect((ip, port))
        logging.debug("Created Tcpclient")

    def send(self, msg):
        """wrap socket send fn."""
        logging.debug(msg)
        msg = msg.encode()
        logging.debug("sending: {0}".format(msg))
        socket_ops.send_message(self.conn, msg)

    def recv(self):
        """wrap socket recv fn."""
        data = socket_ops.recv_message(self.conn)
        logging.debug("recv:%s", data)
        return data.decode("utf-8")

    def close(self):
        """Close the tcp socket.
        TODO: Should I call shutdown?"""
        self.conn.close()
