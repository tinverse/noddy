"""Common socket functions for TcpServer and TcpClient"""
INT_SZ = 10


def send_message_size(sock, nbytes):
    sz = b'%10d' % nbytes
    sock.sendall(sz)


def send_message(sock, msg):
    send_message_size(sock, len(msg))
    sock.sendall(msg)


def recv_exactly(sock, size):
    parts = []
    while size > 0:
        part = sock.recv(size)
        if not part:
            raise ConnectionError("Connection closed")
        parts.append(part)
        size -= len(part)
    return b''.join(parts)  # Reassemble the parts


def recv_message_size(sock):
    sz = recv_exactly(sock, 10)
    return int(sz)


def recv_message(sock):
    # Need to know how big the message is in order to get it
    size = recv_message_size(sock)
    return recv_exactly(sock, size)
    # Now read size bytes to get the messages


def smoke_test():
    from socket import socketpair
    s1, s2 = socketpair()
    send_message(s1, b"Hello")
    assert recv_message(s2), b"Hello"
    s1.close()
    s2.close()


smoke_test()
