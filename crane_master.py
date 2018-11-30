import pickle as pk
import threading
import socket
import time
import random
from copy import deepcopy
import _pickle as cpk
from collections import defaultdict
from app.word_count_topology import word_count_topology
from app.third_app import twitter_user_filter_topology
from dfs.mmp_server import MmpServer
from util import Tuple, TupleBatch, CRANE_MASTER_UDP_PORT, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT, \
    CRANE_MAX_INTERVAL


class CraneMaster:
    def __init__(self, topology_num, mmp_list):
        self.topology_list = [word_count_topology, 'haha', twitter_user_filter_topology]
        self.topology_num = topology_num
        self.local_ip = socket.gethostbyname(socket.getfqdn())

        self.udp_receiver_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver_socket.bind(('0.0.0.0', CRANE_MASTER_UDP_PORT))
        self.udp_receiver_socket.settimeout(2)

        self.aggregator_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.aggregator_socket.bind(('0.0.0.0', CRANE_AGGREGATOR_PORT))
        self.aggregator_socket.settimeout(2)

        self.leader = self.local_ip
        self.prefix = "MASTER - [INFO]: "
        self.mmp_list = mmp_list
        self.slaves = [i[0] for i in mmp_list if i[0] != self.local_ip]
        self.root_tup_ts_dict = {}
        self.final_result = {}
        self.final_result = defaultdict(int)
        self.is_running = True

        # Multi thread
        self.udp_receiver_thread = threading.Thread(target=self.udp_receiver)
        self.monitor_thread = threading.Thread(target=self.crane_monitor)
        self.aggregator_thread = threading.Thread(target=self.crane_aggregator)
        self.udp_receiver_thread.start()
        self.aggregator_thread.start()

    def udp_receiver(self):
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
            print(self.prefix, "A scan begins...")
            finished = 0
            root_tup_ts_dict = cpk.loads(cpk.dumps(self.root_tup_ts_dict))
            self.print_result()
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
                        print(self.prefix, 'TupleBatch', tup.uid,
                              ' has been processed more than ', CRANE_MAX_INTERVAL, ' secs. Re-running it...')
                        self.emit(tup, self.topology_num)
            if finished == len(root_tup_ts_dict):
                print(self.prefix, 'All tuples has been fully processed. Fetching final results...')
                self.print_result()
                self.is_running = False

    def crane_aggregator(self):
        while self.is_running:
            try:
                message, addr = self.aggregator_socket.recvfrom(65535)
                msg = pk.loads(message)
                tuple_batch = msg['tup']
                for big_tup in tuple_batch.tuple_list:
                    tup = big_tup.tup
                    self.final_result[tup[0]] += tup[1]
            except socket.timeout:
                continue

    def terminate(self):
        self.udp_receiver_thread.join()
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
            'master': self.local_ip
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def emit(self, tuple_batch, top_num):
        self.root_tup_ts_dict[tuple_batch.uid] = [tuple_batch, time.time(), tuple_batch.uid]
        # Send to VM3 for testing purposes
        next_node_index = random.randint(0, len(self.slaves) - 1)
        self._unicast(top_num, 0, tuple_batch, tuple_batch.uid, tuple_batch.uid,
                      self.slaves[next_node_index], CRANE_SLAVE_UDP_PORT)

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
                if len(tuple_batch.tuple_list) == 250:
                    self.emit(tuple_batch, self.topology_num)
                    tuple_batch = TupleBatch()
        print(self.prefix + 'All tuples transmitted. Spout closed down.')
        self.monitor_thread.start()

    def print_result(self):
        final_result = cpk.loads(cpk.dumps(self.final_result))
        for k, v in final_result.items():
            print(self.prefix, k, ' --- ', v)


if __name__ == '__main__':
    mmp = []
    mmpServer = MmpServer(mmp)
    if mmpServer.start_join():
        mmpServer.run_without_cmd()
    while True:
        cmd = input("There are three applications available: WordCount, "
                    "TwitterUserFilter, Banana. Enter 1-3 to run one of them: ")
        if cmd == '1':
            print('Submitting Application: <WordCount> ......')
            time.sleep(1)
            break
        elif cmd == '2':
            print('Submitting Application: <TwitterUserFilter> ......')
        elif cmd == '3':
            print('Submitting Application: <DonnoWhatToDo> ......')
            time.sleep(1)
            break
        else:
            print("Wrong app num. Try again!")
    start_time = time.time()
    craneMaster = CraneMaster(int(cmd) - 1, mmp)
    craneMaster.start_top()
    craneMaster.terminate()
    print('Our app use ', time.time() - start_time - 2, ' seconds')

