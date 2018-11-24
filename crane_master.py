import pickle as pk
import threading
import socket
from word_count_topology import word_count_topology
from util import CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT


class CraneMaster:
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

    def _multicast(self, cmd, msg, target_list, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        packet = pk.dumps({
            'data': msg,
            'ip': self.local_ip,
        })
        for i in target_list:
            skt.sendto(packet, (i, port))
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


if __name__ == '__main__':
    while True:
        cmd = input("There are one applications available: WordCount. Enter 1-3 to run one of them: ")
        if cmd == '1':
            break
        else:
            print("Wrong app num. Try again!")

    craneMaster = CraneMaster()

