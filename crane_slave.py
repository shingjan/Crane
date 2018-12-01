import pickle as pk
import threading
import socket
from dfs.mmp_server import MmpServer
from app.word_count_topology import word_count_topology
from app.twitter_user_filter import twitter_user_filter_topology
from app.page_rank_topology import page_rank_topology
from util import CRANE_MASTER_ACK_PORT, CRANE_SLAVE_PORT


class Collector:
    def __init__(self, master):
        self.master = master

    def udp_unicast(self, topology, bolt, tup, rid, xor_id, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'xor_id': xor_id,
            'master': self.master
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def _unicast(self, topology, bolt, tup, rid, xor_id, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'xor_id': xor_id,
            'master': self.master
        })
        connected = False
        # print('try to connect to ip: ', ip, ' with port', port)
        while not connected:
            try:
                skt.connect((ip, port))
                connected = True
            except socket.timeout:
                pass
        skt.sendall(packet)
        skt.shutdown(socket.SHUT_RDWR)
        skt.close()

    def set_master(self, master):
        self.master = master

    def emit(self, top_num, bolt_num, big_tup, rid, xor_id, recv_ip, recv_port):
        print('emit message to', recv_ip)
        self._unicast(top_num, bolt_num, big_tup, rid, xor_id, recv_ip, recv_port)

    def ack(self, rid, xor_id):
        self._unicast(None, None, None, rid, xor_id, self.master, CRANE_MASTER_ACK_PORT)


class CraneSlave:
    def __init__(self, membership_list):
        self.membership_list = membership_list
        self.topology_list = [word_count_topology, page_rank_topology, twitter_user_filter_topology]
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.slave_receiver_thread = threading.Thread(target=self.slave_recevier)
        # self.slave_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.slave_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.slave_receiver_socket.bind(('0.0.0.0', CRANE_SLAVE_PORT))
        self.slave_receiver_socket.settimeout(2)
        self.slave_receiver_socket.listen(5)

        self.master = None
        self.prefix = "SLAVE - [INFO]: "
        self.collector = Collector(None)

    def run(self):
        self.slave_receiver_thread.start()

    def terminate(self):
        self.slave_receiver_thread.join()

    def slave_recevier(self):
        while True:
            try:
                conn, addr = self.slave_receiver_socket.accept()
                chunks = []
                while True:
                    content = conn.recv(1024)
                    if not content:
                        break  # EOF
                    chunks.append(content)
                msg = pk.loads(b''.join(chunks))
                # message, addr = self.slave_receiver_socket.recvfrom(65535)
                # msg = pk.loads(message)
                self.exec_msg(msg)
            except socket.timeout:
                continue

    def exec_msg(self, msg):
        top_num = msg['topology']
        bolt_num = msg['bolt']
        tuple_batch = msg['tup']
        rid = msg['rid']
        xor_id = msg['xor_id']
        master_ip = msg['master']
        self.master = master_ip
        self.collector.set_master(master_ip)
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
