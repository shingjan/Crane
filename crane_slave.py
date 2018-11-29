import pickle as pk
import threading
import socket
from dfs.mmp_server import MmpServer
from app.word_count_topology import word_count_topology
from app.third_app import twitter_user_filter_topology
from util import CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT


class Collector:
    def __init__(self, mmp_list):
        self.mmp_list = mmp_list

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

    def emit(self, top_num, bolt_num, big_tup, rid, xor_id, recv_ip, recv_port):
        self._unicast(top_num, bolt_num, big_tup, rid, xor_id, recv_ip, recv_port)

    def ack(self, rid, xor_id):
        leader = self.mmp_list[0][0]
        self._unicast(None, None, None, rid, xor_id, leader, CRANE_MASTER_UDP_PORT)


class CraneSlave:
    def __init__(self, membership_list):
        self.membership_list = membership_list
        self.topology_list = [word_count_topology, 'haha', twitter_user_filter_topology]
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.udp_receiver_thread = threading.Thread(target=self.udp_recevier)
        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_SLAVE_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

        self.leader = '172.22.158.208'
        self.prefix = "SLAVE - [INFO]: "
        self.collector = Collector(self.leader)

    def run(self):
        self.udp_receiver_thread.start()

    def terminate(self):
        self.udp_receiver_thread.join()

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
        tuple_batch = msg['tup']
        rid = msg['rid']
        xor_id = msg['xor_id']
        curr_bolt = self.topology_list[top_num].bolt_list[bolt_num]
        curr_bolt.execute(top_num, bolt_num, rid, xor_id, tuple_batch, self.collector, self.membership_list)


if __name__ == '__main__':
    mmp = []
    mmpServer = MmpServer(mmp)
    if mmpServer.start_join():
        mmpServer.run()
    else:
        print('SLAVE - [INFO]: mmp server not configured properly. Abort!')
    craneSlave = CraneSlave(mmp)
    craneSlave.run()
    craneSlave.terminate()
