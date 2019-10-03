import threading
import time

from algo.config import CLUSTER_SERVERS
from algo.raft import Raft
from algo.controller import LogEntry


def test_leader_election():
    """Should elect a leader in 0.6s"""
    servers = [Raft(i) for i in range(len(CLUSTER_SERVERS))]
    threads = []
    for server in servers:
        t = threading.Thread(target=server.run)
        threads.append(t)
        t.start()
    niter = 0
    while True:
        time.sleep(0.5)
        niter += 1
        if niter >= 10:
            assert False
        counts = {}
        for server in servers:
            key = server.current_state.name
            if key in counts:
                counts[key] += 1
            else:
                counts[key] = 1
        if 'LEADER' in counts and \
                'FOLLOWER' in counts and \
                counts['LEADER'] == 1 and \
                counts['FOLLOWER'] == len(servers) - 1:
            assert True
            break

    leader_id = [
        server.server_id for server in servers
        if server.current_state.name == 'LEADER'
    ][0]
    servers[leader_id].algo.log.append(LogEntry(server.algo.current_term, 1))
    replicated = [False for i in CLUSTER_SERVERS]
    niter = 0
    while True:
        niter += 1
        time.sleep(0.5)
        if niter >= 10:
            assert False
        for server in servers:
            if (not replicated[server.server_id]
                ) and server.algo.log == servers[leader_id].algo.log:
                replicated[server.server_id] = True
        if all(replicated):
            assert True
            break

    for server in servers:
        threading.Thread(target=server.shutdown).start()

    for t in threads:
        t.join()


def test_log_replication():
    """"""
