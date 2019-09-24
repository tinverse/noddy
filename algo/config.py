CLUSTER_SERVERS = {
    0: ('localhost', 15000),
    1: ('localhost', 15001),
    2: ('localhost', 15002),
    3: ('localhost', 15003),
    4: ('localhost', 15004)
}

CLIENT_FACING_SERVERS = {
    0: ('localhost', 15005),
    1: ('localhost', 15006),
    2: ('localhost', 15007),
    3: ('localhost', 15008),
    4: ('localhost', 15009)
}

HEARTBEAT_TIMEOUT_MS = 50
ELECTION_TIMEOUT_MIN_MS = 150
ELECTION_TIMEOUT_MAX_MS = 300
