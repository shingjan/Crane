import pickle as pk
import threading
import socket
import random
from dfs.mmp_server import MmpServer
from app.word_count_topology import word_count_topology
from app.twitter_user_filter import twitter_user_filter_topology
from app.page_rank_topology import page_rank_topology
from util import CRANE_SLAVE_PORT, CRANE_AGGREGATOR_PORT


class Collector:
    def __init__(self, mmp_list):
        self.mmp_list = mmp_list
        self.prefix = "COLLECTOR - [INFO]: "
        self.master = mmp_list[0][0]

    def udp_unicast(self, topology, bolt, tup, rid, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'master': self.master
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def _unicast(self, topology, bolt, tup, rid, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'master': self.master
        })
        try:
            skt.connect((ip, port))
            total_sent = 0
            # skt.send(pk.dumps(len(packet)))
            while total_sent < len(packet):
                sent = skt.send(packet[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent = total_sent + sent
            skt.shutdown(socket.SHUT_RDWR)
        except ConnectionRefusedError:
            print(self.prefix, "Connection Refused with ", ip, " Emit abort.")
        skt.close()

    def set_master(self, master):
        self.master = master

    def emit(self, top_num, bolt_num, big_tup, rid):
        recv_ip = random.random.randint(1, len(self.mmp_list) - 1)
        print(self.prefix, 'emit message to', recv_ip)
        self._unicast(top_num, bolt_num, big_tup, rid, recv_ip, CRANE_SLAVE_PORT)

    def ack(self, top_num, bolt_num, big_tup, rid):
        print(self.prefix, 'emit message to', self.master)
        self._unicast(top_num, bolt_num, big_tup, rid, self.master, CRANE_AGGREGATOR_PORT)


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
        self.slave_receiver_socket.listen(10)

        self.master = None
        self.prefix = "SLAVE - [INFO]: "
        self.collector = Collector(self.membership_list)

    def run(self):
        self.slave_receiver_thread.start()

    def terminate(self):
        self.slave_receiver_thread.join()

    def slave_recevier(self):
        while True:
            try:
                # message, addr = self.slave_receiver_socket.recvfrom(65535)
                # msg = pk.loads(message)
                conn, addr = self.slave_receiver_socket.accept()
                chunks = []
                # bytes_recv = conn.recv(6)
                # total_length = pk.loads(bytes_recv)
                bytes_recd = 0
                while True:
                    content = conn.recv(1024)
                    if not content:
                        break  # EOF
                    chunks.append(content)
                    bytes_recd += len(content)
                try:
                    msg = pk.loads(b''.join(chunks))
                except EOFError:
                    print(self.prefix, 'Connection interrupted. Abort')
                    continue
                conn.close()
                self.exec_msg(msg)
            except socket.timeout:
                continue

    def exec_msg(self, msg):
        top_num = msg['topology']
        bolt_num = msg['bolt']
        tuple_batch = msg['tup']
        rid = msg['rid']
        master_ip = msg['master']
        self.master = master_ip
        self.collector.set_master(master_ip)
        curr_bolt = self.topology_list[top_num].bolt_list[bolt_num]
        curr_bolt.execute(top_num, bolt_num, rid, tuple_batch, self.collector, self.membership_list)


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
