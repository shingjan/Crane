import pickle as pk
import threading
import socket
from word_count_topology import word_count_topology
from util import CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT


class CraneSlave:
    def __init__(self):
        self.topology_list = [word_count_topology]
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_MASTER_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

    def _unicast(self, topology, bolt, tup, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tuple': tup,
            'ip': self.local_ip,
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def udp_recevier(self):
        while True:
            try:
                message, addr = self.udp_receiver_socket.recvfrom(65535)
                msg = pk.loads(message)
                self.console_log(msg)
            except socket.timeout:
                continue

    def console_log(self, msg):
        print(msg)

