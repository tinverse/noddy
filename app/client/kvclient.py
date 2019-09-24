#!/usr/bin/env python3
"""The Key Val Client"""
import logging
import socket

from net.tcp_client import TcpClient

class _KVClient:
    """Mixin class. Don't instantiate. Talks to the Key Val server"""

    def __init__(self):
        super().__init__()
        logging.debug("Created KVClient")

    def set(self, key, value):
        """Invoke send. Msg format: set key val"""
        cmd = "set {0} {1}".format(key, value)
        self.send(cmd)

    def get(self, key):
        """Invoke recv. Msg format: get key"""
        cmd = "get {}".format(key)
        self.send(cmd)
        return self.recv()

    def delete(self, key):
        """Invoke send Msg format: delete key"""
        cmd = "delete {}".format(key)
        self.send(cmd)

    def close(self):
        """Invoke close on the mixin instance"""
        self.close()


class KVTcpClient(TcpClient, _KVClient):
    """The finished kvclient"""
    def __init__(self, ip, port):
        super().__init__(ip, port)


def main():
    """Entry point"""
    try:
        kvclient = KVTcpClient("localhost", 30000)
        kvclient.set("a", "1")
        for i in range(10000):
            kvclient.set(i, i+1)
            #kvclient.get(i)
        logging.debug("a: %s", kvclient.get("a"))
        kvclient.close()
    except socket.error as ex:
        logging.debug(ex)

if __name__ == "__main__":
    main()
