import pickle as pk
import threading
import socket
import time
from app.word_count_topology import word_count_topology
from dfs.env import INDEX_LIST
from util import Tuple, TupleBatch, CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT, CRANE_MAX_INTERVAL


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
        self.slaves = [INDEX_LIST[i] for i in range(1, 10)]
        self.root_tup_ts_dict = {}
        self.final_result = {}
        self.is_running = True

        # Multi thread
        self.udp_recevier_thread = threading.Thread(target=self.udp_recevier)
        self.monitor_thread = threading.Thread(target=self.crane_monitor)
        self.aggregator_thread = threading.Thread(target=self.crane_aggregator)
        self.udp_recevier_thread.start()
        self.aggregator_thread.start()

    def udp_recevier(self):
        while self.is_running:
            try:
                message, addr = self.udp_receiver_socket.recvfrom(65535)
                msg = pk.loads(message)
                rid = msg['rid']
                old_rid = self.root_tup_ts_dict[rid][2]
                self.root_tup_ts_dict[rid][2] = old_rid ^ msg['xor_id']
            except socket.timeout:
                continue

    def crane_monitor(self):
        while self.is_running:
            time.sleep(2)
            finished = 0
            root_tup_ts_dict = self.root_tup_ts_dict.copy()
            for rid in root_tup_ts_dict:
                tup = root_tup_ts_dict[rid][0]
                time_stamp = root_tup_ts_dict[rid][1]
                total_rid = root_tup_ts_dict[rid][2]
                if total_rid == 0:
                    finished += 1
                    continue
                else:
                    time_spent = time.time() - time_stamp
                    if time_spent >= CRANE_MAX_INTERVAL:
                        print(self.prefix, 'Tuple ', tup, ' has been processed more than 30 secs. Re-running it...')
                        self.emit(tup, self.topology_num)
            if finished == len(root_tup_ts_dict):
                print(self.prefix, 'All tuples has been fully processed. Fetching results...')
                print(self.final_result)
                self.is_running = False

    def crane_aggregator(self):
        while self.is_running:
            try:
                message, addr = self.aggregator_socket.recvfrom(65535)
                msg = pk.loads(message)
                tuple_batch = msg['tup']
                for big_tup in tuple_batch.tuple_list:
                    tup = big_tup.tup
                    print(self.prefix, tup)
                    self.final_result[tup[0]] = tup[1]
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

    def emit(self, tuple_batch, top_num):
        self.root_tup_ts_dict[tuple_batch.uid] = [tuple_batch, time.time(), tuple_batch.uid]
        # Send to VM3 for testing purposes
        self._unicast(top_num, 0, tuple_batch, tuple_batch.uid, tuple_batch.uid, "172.22.156.209", CRANE_SLAVE_UDP_PORT)

    def start_top(self):
        curr_top = self.topology_list[self.topology_num]
        print(self.prefix, curr_top.name, " starting...")
        tuple_batch = TupleBatch()
        while True:
            tup = curr_top.spout.next_tup()
            if not tup:
                self.emit(tuple_batch, self.topology_num)
                break
            else:
                big_tuple = Tuple(tup)
                tuple_batch.add_tuple(big_tuple)
                if len(tuple_batch.tuple_list) == 500:
                    self.emit(tuple_batch, self.topology_num)
                    tuple_batch = TupleBatch()
        print(self.prefix + 'All tuples transmitted. Spout closed down.')
        self.monitor_thread.start()


if __name__ == '__main__':
    while True:
        cmd = input("There are three applications available: WordCount, "
                    "TwitterUserFilter, Banana. Enter 1-3 to run one of them: ")
        if cmd == '1':
            print('Submitting Application: <WordCount> ......')
            time.sleep(1)
            break
        else:
            print("Wrong app num. Try again!")
    start_time = time.time()
    craneMaster = CraneMaster(int(cmd) - 1)
    craneMaster.start_top()
    craneMaster.termiante()
    print('Our app use ', time.time() - start_time - 2, ' seconds')

