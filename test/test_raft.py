import threading
import time

from algo.config import CLUSTER_SERVERS
from algo.raft import Raft


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

    for server in servers:
        threading.Thread(target=server.shutdown).start()

    for t in threads:
        t.join()
