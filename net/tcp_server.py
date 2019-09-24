"""TcpServer Mixin. Expects self.handle as the handler interface"""
import socket
import logging

from . import socket_ops

class TcpServer:
    """Creates a server socket on an ip:port."""
    def __init__(self, ip, port, backlog):
        """ Create a socket and listen """
        logging.debug("In TcpServer __init__")
        super().__init__()
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Warning: Understand implications of REUSEADDR
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((ip, port))
        self.tcp_socket.listen(backlog)
        logging.debug("Created TcpServer")
        self.conns = []

    def run(self):
        """Accept and call handler"""
        try:
            while True:
                conn, addr = self.tcp_socket.accept()
                self.conns.append(conn)
                # pylint: disable=no-member
                self.handle(conn, addr)
        except KeyboardInterrupt:
            for conn in self.conns:
                conn.shutdown(socket.SHUT_RDWR)
        # How to implicitly call destructors? exit isn't working.

    def send(self, conn, msg):
        """wrap socket send fn."""
        logging.debug(msg)
        msg = msg.encode("utf-8")
        logging.debug("sending: {0}".format(msg))
        socket_ops.send_message(conn, msg)

    def recv(self, conn):
        """wrap socket recv fn."""
        data = socket_ops.recv_message(conn)
        logging.debug("recv:%s", data)
        return data.decode("utf-8")

    def __del__(self):
        self.tcp_socket.close()
