from tcp_client import TcpClient


class EchoClient(TcpClient):
    def __init__(self, ip, port):
        super().__init__(ip, port)


if __name__ == '__main__':
    c = EchoClient('localhost', 30000)
    while True:
        s = input()
        c.send(s)
        echo = c.recv()
        print(echo)
