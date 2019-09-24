from raft.raft import BasicRaftThreadedServer
import time

def xtest_communication():
    servers = [BasicRaftThreadedServer(i) for i in range(5)]
    for server in servers:
        server.run()

    for i in range(5):
        for j in range(5):
            if j != i:
                servers[i].send(j, "Hello, World from %d - %d" %(i, j))

    time.sleep(0.5)

    for i in range(5):
        assert servers[i]._queue.qsize() == 4

    for i in range(5):
        while not servers[i]._queue.empty():
            msg = servers[i]._queue.get()
            assert msg.find("Hello, World") != -1

    for server in servers:
        server.close()
