import pickle as pk
import threading
import socket
import time
import random
import _pickle as cpk
from collections import defaultdict
from app.word_count_topology import word_count_topology
from app.page_rank_topology import page_rank_topology
from app.twitter_user_filter_topology import twitter_user_filter_topology
from dfs.mmp_server import MmpServer
from util import Tuple, TupleBatch, CRANE_SLAVE_PORT, CRANE_AGGREGATOR_PORT, \
    CRANE_MAX_INTERVAL, CRANE_BATCH_SIZE


class CraneMaster:
    def __init__(self, topology_num, mmp_list):
        self.topology_list = [word_count_topology, page_rank_topology, twitter_user_filter_topology]
        self.topology_num = topology_num
        self.local_ip = socket.gethostbyname(socket.getfqdn())

        # self.aggregator_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.aggregator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.aggregator_socket.bind(('0.0.0.0', CRANE_AGGREGATOR_PORT))
        self.aggregator_socket.settimeout(2)
        self.aggregator_socket.listen(10)

        self.leader = self.local_ip
        self.prefix = "MASTER - [INFO]: "
        self.mmp_list = mmp_list
        self.root_tup_ts_dict = {}
        self.final_result = {}
        self.final_result = defaultdict(int)
        self.is_running = True

        # Multi thread
        self.monitor_thread = threading.Thread(target=self.crane_monitor)
        self.aggregator_thread = threading.Thread(target=self.crane_aggregator)
        self.aggregator_thread.start()

    def crane_monitor(self):
        while self.is_running:
            finished = 0
            root_tup_ts_dict = cpk.loads(cpk.dumps(self.root_tup_ts_dict))
            # self.print_result()
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
                        print(self.prefix, 'TupleBatch ', tup.uid,
                              ' processed more than ', CRANE_MAX_INTERVAL, ' secs. Re-running it...')
                        self.emit(tup, self.topology_num)
            if finished == len(root_tup_ts_dict):
                print(self.prefix, 'All tuples has been fully processed. Fetching final results...')
                self.print_result()
                self.is_running = False

    def crane_aggregator(self):
        while self.is_running:
            try:
                conn, addr = self.aggregator_socket.accept()
                chunks = []
                # bytes_recv = conn.recv(4)
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
                    conn.shutdown(socket.SHUT_RDWR)
                    conn.close()
                    continue
                rid = msg['rid']
                tuple_batch = msg['tup']
                ts = tuple_batch.timestamp
                old_ts = self.root_tup_ts_dict[rid][1]
                old_rid = self.root_tup_ts_dict[rid][2]
                if ts != old_ts:
                    print(self.prefix, 'Outdated packets received. Drop')
                    conn.shutdown(socket.SHUT_RDWR)
                    conn.close()
                    continue
                self.root_tup_ts_dict[rid][2] = old_rid ^ rid
                for big_tup in tuple_batch.tuple_list:
                    tup = big_tup.tup
                    print(self.prefix, tup)
                    self.final_result[tup[0]] += tup[1]
            except socket.timeout:
                continue

    def terminate(self):
        self.monitor_thread.join()
        self.aggregator_thread.join()

    def udp_unicast(self, topology, bolt, tup, rid, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'topology': topology,
            'bolt': bolt,
            'tup': tup,
            'rid': rid,
            'master': self.local_ip
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
            'master': self.local_ip
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

    def emit(self, tuple_batch, top_num):
        tuple_batch.timestamp = time.time()
        self.root_tup_ts_dict[tuple_batch.uid] = [tuple_batch, tuple_batch.timestamp, tuple_batch.uid]
        next_node_index = random.randint(1, len(self.mmp_list) - 1)
        self._unicast(top_num, 0, tuple_batch, tuple_batch.uid,
                      self.mmp_list[next_node_index][0], CRANE_SLAVE_PORT)

    def start_top(self):
        curr_top = self.topology_list[self.topology_num]
        print(self.prefix, curr_top.name, " starting...")
        tuple_batch = TupleBatch(time.time())
        while True:
            tup = curr_top.spout.next_tup()
            if not tup:
                self.emit(tuple_batch, self.topology_num)
                break
            else:
                big_tuple = Tuple(tup)
                tuple_batch.add_tuple(big_tuple)
                if len(tuple_batch.tuple_list) >= CRANE_BATCH_SIZE:
                    self.emit(tuple_batch, self.topology_num)
                    tuple_batch = TupleBatch(time.time())
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
                    "PageRank and TwitterUserFilter. Enter 1-3 to run one of them: ")
        if cmd == '1':
            print('Submitting Application: <WordCount> ......')
            time.sleep(1)
            break
        elif cmd == '2':
            print('Submitting Application: <PageRank> ......')
            time.sleep(1)
            break
        elif cmd == '3':
            print('Submitting Application: <TwitterUserFilter> ......')
            time.sleep(1)
            break
        else:
            print("Wrong app num. Try again!")
    start_time = time.time()
    craneMaster = CraneMaster(int(cmd) - 1, mmp)
    craneMaster.start_top()
    craneMaster.terminate()
    print('Our app use ', time.time() - start_time - 2, ' seconds')

