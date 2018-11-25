import pickle as pk
import threading
import socket
from word_count_topology import word_count_topology
from util import CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT


class CraneSlave:
    def __init__(self):
        self.topology_list = [word_count_topology]
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_SLAVE_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

        self.leader = '172.22.154.209'
        self.prefix = "SLAVE - [INFO]: "

    def run(self):
        self.udp_recevier_thread.start()

    def terminate(self):
        self.udp_recevier_thread.join()

    def _unicast(self, topology, bolt, tup, rid, xor_id, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'xor_id': xor_id,
            'terminal': False
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def udp_recevier(self):
        while True:
            try:
                message, addr = self.udp_receiver_socket.recvfrom(65535)
                msg = pk.loads(message)
                self.exec_msg(msg)
            except socket.timeout:
                continue

    def exec_msg(self, msg):
        top_num = msg['topology']
        bolt_num = msg['bolt']
        tup = msg['tup']
        print(tup)
        rid = msg['rid']
        curr_bolt = self.topology_list[top_num].bolt_list[bolt_num]
        curr_bolt.execute(rid, tup, self)

    def emit(self, tup):
        print('slave emit stage')
        self._unicast(None, None, tup, None, None, self.leader, CRANE_AGGREGATOR_PORT)

    def ack(self, tup, rid, xor_id):
        print('slave ack stage')
        self._unicast(None, None, tup, rid, xor_id, self.leader, CRANE_MASTER_UDP_PORT)


if __name__ == '__main__':
    craneSlave = CraneSlave()
    craneSlave.run()
    craneSlave.terminate()
