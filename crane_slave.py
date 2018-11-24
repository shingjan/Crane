import pickle as pk
import threading
import socket
from word_count_topology import word_count_topology
from util import CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT

class CraneSlave:
    def __init__(self):
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_MASTER_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

    def _unicast(self, cmd, msg, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        packet = pk.dumps({
            'data': msg,
            'ip': self.local_ip,
        })
        skt.sendto(packet, (ip, port))
        skt.close()

