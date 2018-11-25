import pickle as pk
import threading
import socket
import time
from word_count_topology import word_count_topology
from util import Tuple, IP_LIST, CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT, CRANE_MAX_INTERVAL


class CraneMaster:
    def __init__(self, topology_num):
        self.topology_list = [word_count_topology]
        self.topology_num = topology_num
        self.local_ip = socket.gethostbyname(socket.getfqdn())

        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_MASTER_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

        self.aggregator_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.aggregator_socket.bind(('0.0.0.0', CRANE_AGGREGATOR_PORT))
        self.aggregator_socket.settimeout(2)

        self.leader = '172.22.154.209'
        self.prefix = "MASTER - [INFO]: "
        self.slaves = [list(IP_LIST.keys())[i] for i in range(1, 10)]
        self.root_tup_ts_dict = {}
        self.final_result = []

        # Multi thread
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.monitor_thread = threading.Thread(target=self.crane_monitor)
        self.aggregator_thread = threading.Thread(target=self.crane_aggregator)
        self.udp_recevier_thread.start()
        self.aggregator_thread.start()

    def udp_recevier(self):
        while True:
            try:
                message, addr = self.udp_receiver_socket.recvfrom(65535)
                msg = pk.loads(message)
                rid = msg['rid']
                print(rid)
                print(msg['xor_id'])
                old_rid = self.root_tup_ts_dict[rid][2]
                self.root_tup_ts_dict[rid][2] = old_rid ^ msg['xor_id']
            except socket.timeout:
                continue

    def crane_monitor(self):
        while True:
            time.sleep(3)
            finished = 0
            for rid in list(self.root_tup_ts_dict):
                tup = self.root_tup_ts_dict[rid][0]
                time_stamp = self.root_tup_ts_dict[rid][1]
                total_rid = self.root_tup_ts_dict[rid][2]
                if total_rid == 0:
                    finished += 1
                    continue
                else:
                    time_spent = time.time() - time_stamp
                    if time_spent >= CRANE_MAX_INTERVAL:
                        print(self.prefix, 'Tuple ', tup, ' has been processed more than 10 secs. Re-running it...')
                        self.emit(tup, self.topology_num)
            if finished == len(self.root_tup_ts_dict):
                print(self.prefix, 'All tuples has been fully processed. Fetching results...')
                print(self.final_result)
                break

    def crane_aggregator(self):
        while True:
            try:
                message, addr = self.aggregator_socket.recvfrom(65535)
                msg = pk.loads(message)
                self.final_result.append(msg['tup'])
            except socket.timeout:
                continue

    def termiante(self):
        self.udp_recevier_thread.join()
        self.monitor_thread.join()
        self.aggregator_thread.join()

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

    def emit(self, tup, top_num):
        # print(tup)
        big_tuple = Tuple(tup)
        self.root_tup_ts_dict[big_tuple.uid] = [tup, time.time(), big_tuple.uid]
        # Send to VM3 for testing purposes
        self._unicast(top_num, 0, tup, big_tuple.uid, 0, "172.22.156.209", CRANE_SLAVE_UDP_PORT)

    def start_top(self):
        curr_top = self.topology_list[self.topology_num]
        print(curr_top.name, " starting...")
        while True:
            tup = curr_top.spout.next_tup()
            if not tup:
                break
            self.emit(tup, self.topology_num)
        print(self.prefix + 'All tuples transmitted. Spout closed down.')
        self.monitor_thread.start()


if __name__ == '__main__':
    while True:
        cmd = input("There are one applications available: WordCount, Banana, Potato. Enter 1-3 to run one of them: ")
        if cmd == '1':
            print('Application <WordCount> is chosen. Good Choice!')
            break
        else:
            print("Wrong app num. Try again!")

    craneMaster = CraneMaster(int(cmd) - 1)
    craneMaster.start_top()
    craneMaster.termiante()

