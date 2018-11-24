import pickle as pk
import threading
import socket
from word_count_topology import word_count_topology
from util import IP_LIST, CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT


class CraneMaster:
    def __init__(self):
        self.topology_list = [word_count_topology]
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_MASTER_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)
        self.leader = '172.22.158.208'
        self.slaves =[ list(IP_LIST.keys())[i] for i in range(1,10)]
        self.event_dict = {}

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

    def _multicast(self, topology, bolt, tup, target_list, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tuple': tup,
            'ip': self.local_ip,
        })
        for i in target_list:
            skt.sendto(packet, (i, port))
        skt.close()

    def send_tup(self, tup):
        print(tup)

    def start_top(self, top_num):
        curr_top = self.topology_list[top_num]
        print(curr_top.name, " starting...")
        while True:
            tup = curr_top.spout.next_tup()
            if not tup:
                break
            self.send_tup(tup)
        print('Topology successfully executed. Shutdown')


if __name__ == '__main__':
    while True:
        cmd = input("There are one applications available: WordCount, Banana, Potato. Enter 1-3 to run one of them: ")
        if cmd == '1':
            print('Application WordCount is chosen. Good Choice!')
            break
        else:
            print("Wrong app num. Try again!")

    craneMaster = CraneMaster()
    craneMaster.start_top(int(cmd) - 1)

