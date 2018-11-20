import threading
import socket

IP_LIST = {
        "172.22.158.208": 1,
        "172.22.154.209": 2,
        "172.22.156.209": 3,
        "172.22.158.209": 4,
        "172.22.154.210": 5,
        "172.22.156.210": 6,
        "172.22.158.210": 7,
        "172.22.154.211": 8,
        "172.22.156.211": 9,
        "172.22.158.211": 10}

LOCK_LIST = {
        "172.22.158.208": threading.Event(),
        "172.22.154.209": threading.Event(),
        "172.22.156.209": threading.Event(),
        "172.22.158.209": threading.Event(),
        "172.22.154.210": threading.Event(),
        "172.22.156.210": threading.Event(),
        "172.22.158.210": threading.Event(),
        "172.22.154.211": threading.Event(),
        "172.22.156.211": threading.Event(),
        "172.22.158.211": threading.Event()}

INDEX_LIST = {
        1: "172.22.158.208",
        2: "172.22.154.209",
        3: "172.22.156.209",
        4: "172.22.158.209",
        5: "172.22.154.210",
        6: "172.22.156.210",
        7: "172.22.158.210",
        8: "172.22.154.211",
        9: "172.22.156.211",
        10: "172.22.158.211"}


UDP_SOCKET = 9200
UDP_LS_SOCKET = 9203
UDP_RECV_SOCKET = 9208
UDP_REQ_SOCKET = 9205
MMP_TCP_PORT = 7777
CLIENT_TCP_PORT = 6666
DFS_TCP_PORT = 8888

NUM_MMP_SOCKETS = 9
MMP_SOCKET_LIST = []
for i in range(NUM_MMP_SOCKETS):
    temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    temp.bind(('0.0.0.0', 9000 + i))
    temp.settimeout(2)
    MMP_SOCKET_LIST.append(temp)

MMP_SOCKET_DICT = {
        'send': (MMP_SOCKET_LIST[0], 0),
        'ack': (MMP_SOCKET_LIST[1], 1),
        'decommission': (MMP_SOCKET_LIST[2], 2),
        'join': (MMP_SOCKET_LIST[3], 3),
        'mmp': (MMP_SOCKET_LIST[4], 4),
        'elect': (MMP_SOCKET_LIST[5], 5),
        'leader': (MMP_SOCKET_LIST[6], 6),
        'ask': (MMP_SOCKET_LIST[7], 7),
        'info': (MMP_SOCKET_LIST[8], 8)}

NUM_TCP_SOCKETS = 12
DFS_SOCKET_LIST = []
for i in range(NUM_TCP_SOCKETS):
    temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    temp.bind(('0.0.0.0', 9100 + i))
    temp.settimeout(2)
    DFS_SOCKET_LIST.append(temp)

DFS_SOCKET_DICT = {
        'get': (DFS_SOCKET_LIST[0], 0),
        'put': (DFS_SOCKET_LIST[1], 1),
        'del': (DFS_SOCKET_LIST[2], 2),
        'ls': (DFS_SOCKET_LIST[3], 3),
        'getv': (DFS_SOCKET_LIST[4], 4),
        'req': (DFS_SOCKET_LIST[5], 5),
        'repair': (DFS_SOCKET_LIST[6], 6),
        'dict': (DFS_SOCKET_LIST[7], 7),
        'recv': (DFS_SOCKET_LIST[8], 8),
        'ask_dict': (DFS_SOCKET_LIST[9], 9),
        'del_file': (DFS_SOCKET_LIST[10], 10),
        'get_all': (DFS_SOCKET_LIST[11], 11)}


