import time
import socket
import logging
import os
import pickle as pk
import threading
import select
import glob
#from shutil import copy


# TODO: build, delete file
class DfsServer:
    def __init__(self):
        # ----------------------------
        # membership server params
        # ----------------------------
        self.membership_list = []
        self.neighbors = []
        self.ip_list = {
            "172.22.158.208": 1,
            "172.22.154.209": 2,
            "172.22.156.209": 3,
            "172.22.158.209": 4,
            "172.22.154.210": 5,
            "172.22.156.210": 6,
            "172.22.158.210": 7,
            "172.22.154.211": 8,
            "172.22.156.211": 9,
            "172.22.158.211": 10}
        self.lock_list = {
            "172.22.158.208": threading.Event(),
            "172.22.154.209": threading.Event(),
            "172.22.156.209": threading.Event(),
            "172.22.158.209": threading.Event(),
            "172.22.154.210": threading.Event(),
            "172.22.156.210": threading.Event(),
            "172.22.158.210": threading.Event(),
            "172.22.154.211": threading.Event(),
            "172.22.156.211": threading.Event(),
            "172.22.158.211": threading.Event()}
        self.index_list = {
                1: "172.22.158.208",
                2: "172.22.154.209",
                3: "172.22.156.209",
                4: "172.22.158.209",
                5: "172.22.154.210",
                6: "172.22.156.210",
                7: "172.22.158.210",
                8: "172.22.154.211",
                9: "172.22.156.211",
                10: "172.22.158.211"}

        self.mmp_sockets = 9
        self.mmp_socket_list = []
        for i in range(self.mmp_sockets):
            temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp.bind(('0.0.0.0', 9000 + i))
            temp.settimeout(2)
            self.mmp_socket_list.append(temp)

        self.mmp_socket_dict = {
            'send': (self.mmp_socket_list[0], 0),
            'ack': (self.mmp_socket_list[1], 1),
            'decommission': (self.mmp_socket_list[2], 2),
            'join': (self.mmp_socket_list[3], 3),
            'mmp': (self.mmp_socket_list[4], 4),
            'elect': (self.mmp_socket_list[5], 5),
            'leader': (self.mmp_socket_list[6], 6),
            'ask': (self.mmp_socket_list[7], 7),
            'info': (self.mmp_socket_list[8], 8),
        }
        self.leader = None
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.is_running = False
        self.mmp_receiver = threading.Thread(target=self.mmp_receiver_thread)
        self.mmp_sender = threading.Thread(target=self.mmp_sender_thread)
        self.dfs_receiver = threading.Thread(target=self.dfs_receiver_thread)
        self.cmd = threading.Thread(target=self.cmd_thread)
        # ----------------------------
        # init logger: level INFO used
        # ----------------------------
        self.logger = logging.getLogger('dfs')
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler('../dfs.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        # ----------------------------
        # sdfs server params
        # file_dict:
        #   Key: sdfs_name
        #   Value: (primary_node: ip, num_versions: [],
        # ----------------------------
        self.file_dict = {}
        # ----------------------------
        # key: filename
        # value: versions, tuple of str
        # ----------------------------
        self.local_file_dict = {}
        self.file_dir = "../dfs/"
        self.tmp_file_dir = "../tmp/"
        self.delimiter = "-"
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_port = 8888
        self.client_tcp_port = 6666
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.settimeout(2)
        self.tcp_socket.listen(10)
        self.dfs_sockets = 12
        self.dfs_socket_list = []
        self.dfs_socket_dict = {}
        for i in range(self.dfs_sockets):
            temp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp.bind(('0.0.0.0', 9100 + i))
            temp.settimeout(2)
            self.dfs_socket_list.append(temp)
        self.dfs_socket_dict = {
            'get': (self.dfs_socket_list[0], 0),
            'put': (self.dfs_socket_list[1], 1),
            'del': (self.dfs_socket_list[2], 2),
            'ls': (self.dfs_socket_list[3], 3),
            'getv': (self.dfs_socket_list[4], 4),
            'req': (self.dfs_socket_list[5], 5),
            'repair': (self.dfs_socket_list[6], 6),
            'dict': (self.dfs_socket_list[7], 7),
            'recv': (self.dfs_socket_list[8], 8),
            'ask_dict': (self.dfs_socket_list[9], 9),
            'del_file': (self.dfs_socket_list[10], 10),
            'get_all': (self.dfs_socket_list[11], 11)
        }
    '''
    -----------------------------------------------------------------------
                              Mmp Helper functions
    -----------------------------------------------------------------------
    '''
    def run(self):
        self.mmp_receiver.start()
        self.mmp_sender.start()
        self.dfs_receiver.start()
        self.cmd.start()

    def terminate(self):
        self.mmp_receiver.join()
        self.mmp_sender.join()
        self.dfs_receiver.join()
        self.cmd.join()

    def _unicast(self, cmd, msg, ip, port, flag):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        if flag:
            sender_port = 9000 + self.mmp_socket_dict[cmd][1]
        else:
            sender_port = 9100 + self.dfs_socket_dict[cmd][1]
        packet = pk.dumps({
            'cmd': cmd,
            'data': msg,
            'ip': self.local_ip,
            'sender_port': sender_port,
            'sender_timestamp': time.time()
        })
        #print(cmd, " send to", ip, " via port ", port)
        skt.sendto(packet, (ip, port))
        skt.close()

    def _multicast(self, cmd, msg, target_list, port, flag):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        if flag:
            sender_port = 9000 + self.mmp_socket_dict[cmd][1]
        else:
            sender_port = 9100 + self.dfs_socket_dict[cmd][1]
        packet = pk.dumps({
            'cmd': cmd,
            'data': msg,
            'ip': self.local_ip,
            'sender_port': sender_port,
            'sender_timestamp': time.time()
        })
        for i in target_list:
            #print(cmd, " send to", i, " via port ", port)
            skt.sendto(packet, (i, port))
        skt.close()

    def _elect(self):
        m = 10
        member_hosts = [i[0] for i in self.membership_list]
        new_leader = None
        if not member_hosts:
            self.leader = self.local_ip
            self.membership_list = [(self.local_ip, time.time())]
        for i in member_hosts:
            if self.ip_list[i] < m:
                m = self.ip_list[i]
                new_leader = i
        if new_leader:
            self._unicast("elect", "", new_leader, 9000 + self.mmp_socket_dict["elect"][1], True)

    def _update_neighbors(self):
        length = len(self.membership_list)
        if length < 4:
            self.neighbors = [i[0] for i in self.membership_list if i[0] != self.local_ip]
        else:
            member_hosts = [i[0] for i in self.membership_list]
            idx = member_hosts.index(self.local_ip)
            add = 1
            self.neighbors = []
            while len(self.neighbors) < 3:
                if (idx+add) % length != idx:
                    self.neighbors.append(member_hosts[(idx+add) % length])
                add += 1

    '''
    -----------------------------------------------------------------------
                            Request Handlers
    -----------------------------------------------------------------------
    '''
    def start_join(self):
        self._reset()
        self._multicast('ask', "",
                        [i for i in self.ip_list.keys() if i != self.local_ip],
                        9000 + self.mmp_socket_dict['ask'][1], True)
        try:
            message, addr = self.mmp_socket_dict['info'][0].recvfrom(65536)
            msg = pk.loads(message)
            self.leader = msg['data']
            self._unicast('join', (self.local_ip, time.time()), self.leader, 9000 + self.mmp_socket_dict['join'][1], True)
            try:
                message1, addr1 = self.mmp_socket_dict['mmp'][0].recvfrom(65536)
                msg1 = pk.loads(message1)
                self.logger.info("Updating mmp list from leader: " + msg1['ip'])
                self.membership_list = msg1['data']
                self._update_neighbors()
            except socket.timeout:
                self.logger.info("No response from leader. Abort")
                return False
        except socket.timeout:
            self.leader = self.local_ip
            self.membership_list.append((self.local_ip, time.time()))
            self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
            self.logger.info(self.local_ip + " is added to the group as leader")
        self.is_running = True
        return True

    def decommission(self):
        self._multicast('decommission', (self.local_ip, time.time()),
                        [i[0] for i in self.membership_list if i[0] != self.local_ip],
                        9000 + self.mmp_socket_dict['decommission'][1], True)
        self.membership_list = []
        self.neighbors = []
        self.leader = None
        self.is_running = False
        self._reset()

    def join_request(self, msg):
        if not self._if_in_mmp(msg['data'][0]):
            if self.local_ip == self.leader:
                self.membership_list.append(msg['data'])
                self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
                self._unicast('mmp', self.membership_list, msg['data'][0],
                              9000 + self.mmp_socket_dict['mmp'][1], True)
                #print("mmp list send to", msg['data'][0])
                time.sleep(1)
                self._multicast('join', msg['data'], [i[0] for i in self.membership_list if i[0] != self.leader],
                                9000 + self.mmp_socket_dict['join'][1], True)
                time.sleep(1)
                for f in self.file_dict.keys():
                    self._start_repair_file(f, msg['data'][0], 'join')
            else:
                self.membership_list.append(msg['data'])
                self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
            self.logger.info(msg['data'][0] + " is added to the group")
            self._update_neighbors()
            self._elect()
            if self.local_ip == self.leader:
                #self._build_file_dict()
                all_affected_files = []
                for f in self.file_dict.keys():
                    all_affected_files.append(f)
                for f in all_affected_files:
                    new_pri = self._hash(f)
                    self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

    def decommission_request(self, msg):
        if self._if_in_mmp(msg['data'][0]):
            members = [i[0] for i in self.membership_list]
            self.membership_list.pop(members.index(msg['data'][0]))
            self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
            self.logger.info(msg['data'][0] + " left the group")
            self._update_neighbors()
        if not self._if_in_mmp(self.leader):
            self._elect()

        if self.leader == self.local_ip:
            self._start_repair_ip(msg['data'][0])
            # TODO no build
            all_affected_files = []
            for f in self.file_dict.keys():
                all_affected_files.append(f)
            for f in all_affected_files:
                new_pri = self._hash(f)
                self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

    def quit_request(self, left_ip):
        member_hosts = [member[0] for member in self.membership_list]
        if left_ip in member_hosts:
            idx = member_hosts.index(left_ip)
            self.membership_list.pop(idx)
            self.membership_list.sort(key=lambda i:self.ip_list[i[0]])
            self.logger.info(left_ip + " is detected left the group")
        self._multicast('decommission', (left_ip, time.time()),
                        [i[0] for i in self.membership_list if i[0] != self.local_ip],
                        9000 + self.mmp_socket_dict['decommission'][1], True)
        self._update_neighbors()
        if not self._if_in_mmp(self.leader):
            self._elect()

        if self.local_ip == self.leader:
            self._start_repair_ip(left_ip)
            # TODO build
            all_affected_files = []
            for f in self.file_dict.keys():
                all_affected_files.append(f)
            for f in all_affected_files:
                new_pri = self._hash(f)
                self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

    '''
    -----------------------------------------------------------------------
                              DFS Helper functions
    -----------------------------------------------------------------------
    '''
    def _reset(self):
        """
        clear all stored files when dec
        """
        if os.path.isdir(self.file_dir):
            fnames = self.file_dir+'*'
            files = glob.glob(fnames)
            for f in files:
                os.remove(f)
        else:
            os.mkdir(self.file_dir)
        self.local_file_dict = {}
        self.file_dict = {}

    def _get_neighbors(self, ip, ls = None):
        if not ls:
            ls = [i[0] for i in self.membership_list]

        length = len(ls)
        neighbors = []
        if length < 4:
            neighbors = [i for i in ls if i != ip]
        else:
            member_hosts = ls
            idx = member_hosts.index(ip)
            add = 1
            while len(neighbors) < 3:
                if (idx+add) % length != idx:
                    neighbors.append(member_hosts[(idx+add) % length])
                add += 1
        return neighbors

    def _if_in_mmp(self, ip):
        member_hosts = [i[0] for i in self.membership_list]
        return ip in member_hosts

    def _hash(self, filename, ls=None):
        """
        Hash sdfsfilename, determine the primary node
        """
        if not ls:
            ls = [i[0] for i in self.membership_list]

        s = 0
        for i in filename:
            s += ord(i)
        s = 1+s % 10
        """
        k + len(self.membership_list)%10 = s + 1
        """
        n = len(ls)
        candidates = [s-n-10, s-n]
        ls.sort(key=lambda i: self.ip_list[i])
        for k in candidates:
            if k >= -n and k < n:
                print(k)
                return ls[k]

    def _start_repair_ip(self, ip):
        """
        Find all files supposed to be in a crashed node ip
        Called by leader
        """
        files = []
        for f in self.file_dict.keys():
            if ip in self.file_dict[f]:
                files.append(f)
        files = list(set(files))
        for f in files:
            self._start_repair_file(f, ip)

    def _start_repair_file(self, sdfs_name, ip, msg='dec'):
        primary_node = self._hash(sdfs_name)
        member_hosts = [i[0] for i in self.membership_list]
        if msg == 'join':
            ori_hosts = [i for i in member_hosts if i!=ip]
        else:
            member_hosts.append(ip)
            ori_hosts = member_hosts
            ori_hosts.sort(key=lambda i: self.ip_list[i])

        ori_primary_node = self._hash(sdfs_name, ori_hosts)
        ori_nb = self._get_neighbors(ori_primary_node, ori_hosts)
        ls = [ori_primary_node]+ori_nb
        self._unicast('repair', (sdfs_name, ls), primary_node, 9100 + self.dfs_socket_dict['repair'][1], False)

    def _repair(self, sdfs_name, ls):
        """
        Re-replication while a node crashed, given sdfsfilename
        1. ask and get file
        2. del all others
        3. send to NBs
        """
        print("Repair started! File will be stored in me", self.local_ip, "and", self.neighbors)
        fnames = self.file_dir + sdfs_name + '*'
        files = glob.glob(fnames)
        member_hosts = [i[0] for i in self.membership_list]
        if not files:
            for ip in ls:
                if ip in member_hosts:
                    self._unicast('get_all', sdfs_name, ip, 9100 + self.dfs_socket_dict['get_all'][1], False)
                    break
        try:
            message, addr = self.dfs_socket_dict['req'][0].recvfrom(65535)
            msg = pk.loads(message)
            versions = msg['data']
            for i in range(versions):
                self._recv_file_from(sdfs_name)
        except socket.timeout:
            print("no file received from get_all req")

        to_del_nodes = [i[0] for i in self.membership_list if i[0] != self.local_ip]
        for ip in to_del_nodes:
            self._unicast('del_file', sdfs_name, ip, 9100 + self.dfs_socket_dict['del_file'][1], False)

        time.sleep(0.5)
        all_files = self._get_all_versions(sdfs_name)
        for ip in self.neighbors:
            print(self.local_ip, " sending ", sdfs_name, " to ", ip)
            for f in all_files:
                self._unicast('recv', sdfs_name, ip, 9100 + self.dfs_socket_dict['recv'][1], False)
                self._send_file_to(f, ip, self.tcp_port)

        print("Repair finished!")

    def _repair_put(self, sdfs_name):
        all_files = self._get_all_versions(sdfs_name)
        all_ver = [int(i[-1]) for i in all_files]
        lastest = max(all_ver)
        for f in all_files:
            if f[-1] == str(lastest):
                lastest_file = f
                break

        for ip in self.neighbors:
            self._unicast('recv', sdfs_name, ip, 9100 + self.dfs_socket_dict['recv'][1], False)
            self._send_file_to(lastest_file, ip, self.tcp_port)

    def _get_all_versions(self, sdfs_name):
        prefix = self.file_dir+sdfs_name+'*'
        all_files = glob.glob(prefix)
        return all_files

    def _get_file_by_name(self, sdfs_name):
        if sdfs_name in self.local_file_dict:
            return self.file_dir + sdfs_name + self.delimiter + "v" + str(self.local_file_dict[sdfs_name][-1])
        else:
            return None

    def _put_file_by_name(self, sdfs_name):
        if sdfs_name in self.local_file_dict:
            latest = self.local_file_dict[sdfs_name][-1]
            latest += 1
            self.local_file_dict[sdfs_name].append(latest)
            return self.file_dir + sdfs_name + self.delimiter + "v" + str(latest)
        else:
            self.local_file_dict[sdfs_name] = [1]
            return self.file_dir + sdfs_name + self.delimiter + "v1"

    def _del_file_by_name(self, sdfs_name):
        if sdfs_name in self.local_file_dict.keys():
            self.local_file_dict.pop(sdfs_name)
            print(self.local_file_dict)
            for f in glob.glob(self.file_dir + sdfs_name + "*"):
                os.remove(f)
        else:
            print("File not found for deletion.")
            return

    def _send_file_to(self, full_local_name, ip, port):
        target = open(full_local_name, "rb")
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = False
        while not connected:
            try:
                skt.connect((ip, port))
                connected = True
            except Exception:
                pass
        while True:
            chunk = target.read(1024)
            if not chunk:
                break  # EOF
            skt.sendall(chunk)
        skt.shutdown(socket.SHUT_RDWR)
        target.close()

    def _send_all_file_to(self, sdfs_name, counter, ip, port):
        versions = self.local_file_dict[sdfs_name]
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        counter = int(counter)
        if counter > 5:
            counter = 5
        count = 0
        connected = False
        while not connected:
            try:
                skt.connect((ip, port))
                connected = True
            except Exception:
                pass
        for version in versions:
            target = open(self.file_dir + sdfs_name + self.delimiter + 'v' + str(version), "rb")
            print(self.file_dir + sdfs_name + self.delimiter + 'v' + str(version) + ";")
            while True:
                chunk = target.read(1024)
                if not chunk:
                    break  # EOF
                skt.sendall(chunk)
            skt.send(self.delimiter.encode())
            target.close()
            count += 1
            if count == counter:
                break
        skt.shutdown(socket.SHUT_RDWR)

    def _recv_file_from(self, sdfs_name):
        full_sdfs_name = self._put_file_by_name(sdfs_name)
        f = open(full_sdfs_name, 'wb')
        client, addr = self.tcp_socket.accept()
        while True:
            content = client.recv(1024)
            if not content:
                break  # EOF
            f.write(content)
        f.close()

    def _update_file_dict(self, d, ip):
        """
        d: local_file_dict received from other nodes
        called by Mr. leader elected
        """
        for f in d.keys():
            if f in self.file_dict and ip not in self.file_dict[f]:
                self.file_dict[f].append(ip)
            else:
                self.file_dict[f] = [ip]

    def _build_file_dict(self):
        """Call by leader"""
        self.file_dict = {}
        for f in self.local_file_dict.keys():
            if f in self.file_dict:
                self.file_dict[f].append(self.local_ip)
            else:
                self.file_dict[f] = [self.local_ip]

        member_hosts = [i[0] for i in self.membership_list if i != self.leader]
        self._multicast('ask_dict', '', member_hosts, 9100 + self.dfs_socket_dict['ask_dict'][1], False)

    '''
    -----------------------------------------------------------------------
                                Thread lambdas
    -----------------------------------------------------------------------
    '''
    def mmp_sender_thread(self):
        self._update_neighbors()
        while True:
            for ip in self.neighbors:
                if self.is_running:
                    try:
                        self._unicast('send', 'ping from: ' + self.local_ip, ip,
                                      9000 + self.mmp_socket_dict['send'][1], True)
                        time.sleep(0.5)
                        self._unicast('send', 'ping from: ' + self.local_ip, ip,
                                      9000 + self.mmp_socket_dict['send'][1], True)
                        if self.lock_list[ip].wait(2):
                            #self.lock_list[ip].clear()
                            pass
                        else:
                            self.logger.info('No packet received in 2S from ' + ip)
                            self.quit_request(ip)
                        self.lock_list[ip].clear()
                    except (socket.error, socket.gaierror) as err_msg:
                        self.logger.info(err_msg)

    def mmp_receiver_thread(self):
        while True:
            try:
                if self.is_running:
                    readable, _, _ = select.select(self.mmp_socket_list, [], [])
                    for s in readable:
                        msg, address = s.recvfrom(65535)
                        message = pk.loads(msg)
                        self.exec_mmp_message(message, address)
            except socket.timeout:
                continue

    def dfs_receiver_thread(self):
        while True:
            try:
                if self.is_running:
                    readable, _, _ = select.select(self.dfs_socket_list, [], [])
                    for s in readable:
                        msg, address = s.recvfrom(65535)
                        message = pk.loads(msg)
                        self.exec_dfs_message(message, address)
            except socket.timeout:
                continue

    def cmd_thread(self):
        while True:
            cmd = input('Available cmds: ls, self, join, dec, store, ld and exit. Enter: ')
            if cmd == 'join':
                if not self.start_join():
                    print("Rejoin failed. Try rejoin again:")
            elif cmd == 'dec':
                self.decommission()
            elif cmd == 'ls':
                print("Members: ")
                for m in self.membership_list:
                    print(m)
                print("neighbors: ")
                for n in self.neighbors:
                    print(n)
            elif cmd == 'self':
                print(self.local_ip)
            elif cmd == 'ld':
                print(self.leader)
            elif cmd == 'store':
                for i in self.local_file_dict.keys():
                    print("File: ", i, "has versions", self.local_file_dict[i])
            elif cmd == 'gstore':
                if self.local_ip == self.leader:
                    print("Global file dict", self.file_dict)
            elif cmd == 'exit':
                os._exit(0)

            else:
                print("Invalid cmd, enter again:")

    def exec_mmp_message(self, message, address):
        if message['cmd'] == 'send':
            self._unicast('ack', 'ack from: ' + self.local_ip, message['ip'], 9000 + self.mmp_socket_dict['ack'][1], True)
        elif message['cmd'] == 'ack':
            self.lock_list[message['ip']].set()
        elif message['cmd'] == 'decommission':
            self.decommission_request(message)
        elif message['cmd'] == 'join':
            self.join_request(message)
        elif message['cmd'] == 'ask':
            if message['sender_port'] != 9200:
                self._unicast('info', self.leader, message['ip'], 9000 + self.mmp_socket_dict['info'][1], True)
            else:
                # The unicast is to client, whose port is solely on 9200
                self._unicast('info', self.leader, message['ip'], 9200, True)
        elif message['cmd'] == 'elect':
            if self.local_ip != self.leader:
                self.leader = self.local_ip
                member_hosts = [i[0] for i in self.membership_list if i != self.local_ip]
                self._multicast('leader', '', member_hosts, 9000 + self.mmp_socket_dict['leader'][1], True)
                self._multicast('ask_dict', '', member_hosts, 9100 + self.dfs_socket_dict['ask_dict'][1], False)
        elif message['cmd'] == 'leader':
            self.leader = message['ip']
            self._update_neighbors()
            self._build_file_dict()
            # TODO build file dict

    def exec_dfs_message(self, message, address):
        if message['cmd'] == 'ask_dict':
            self._unicast('dict', self.local_file_dict, message['ip'], 9100 + self.dfs_socket_dict['dict'][1], False)
        elif message['cmd'] == 'get':
            primary_node = self._hash(message['data'])
            if self.leader == self.local_ip:
                '''
                Tell client who is the primary node, through port - 9200
                '''
                self._unicast('recv', primary_node, message['ip'], 9200 + self.dfs_socket_dict['recv'][1], False)
            if self.local_ip == primary_node:
                full_sdfs_name = self._get_file_by_name(message['data'])
                print(full_sdfs_name)
                if full_sdfs_name:
                    '''
                    Tell client to receive file through port - 6666
                    '''
                    self._send_file_to(full_sdfs_name, message['ip'], self.client_tcp_port)
                else:
                    print('No file with name: ' + message['data'] + " found on the server")
        elif message['cmd'] == 'put':
            primary_node = self._hash(message['data'])
            if self.local_ip == self.leader:
                self.file_dict[message['data']] = [primary_node] + self._get_neighbors(primary_node)
                '''
                Tell client who is the primary node, through port - 9200
                '''
                self._unicast('req', primary_node, message['ip'], 9200 + self.dfs_socket_dict['req'][1], False)
        elif message['cmd'] == 'del':
            if self.local_ip == self.leader:
                if message['data'] in self.file_dict:
                    self.file_dict.pop(message['data'])
            primary_node = self._hash(message['data'])
            if primary_node == self.local_ip:
                self._del_file_by_name(message['data'])
                self._multicast('del_file', message['data'], self.neighbors, 9100 + self.dfs_socket_dict['del'][1], False)
            else:
                # For leader
                self._unicast('del', message['data'], primary_node, 9100 + self.dfs_socket_dict['del'][1], False)
        elif message['cmd'] == 'ls':
            '''
            Tell client the result of 'ls' cmd, through port - 9200
            '''
            #self._build_file_dict()
            if message['data'] in self.file_dict:
                self._unicast('ls', self.file_dict[message['data']], message['ip'], 9200 + self.dfs_socket_dict['ls'][1], False)
            else:
                self._unicast('ls', [], message['ip'], 9200 + self.dfs_socket_dict['ls'][1], False)

        elif message['cmd'] == 'getv':
            sdfs_name = message['data'][0]
            num_versions = message['data'][1]
            primary_node = self._hash(sdfs_name)
            if self.leader == self.local_ip and primary_node != self.local_ip:
                '''
                Tell client the primary node of that sdfs_file, through port - 9200 + 'req'
                '''
                self._unicast('req', primary_node, message['ip'], 9200 + self.dfs_socket_dict['req'][1], False)
            elif primary_node == self.local_ip:
                if sdfs_name in self.local_file_dict:
                    self._send_all_file_to(sdfs_name, num_versions, message['ip'], self.client_tcp_port)
                else:
                    print("File is not on the dict. Abort.")
        elif message['cmd'] == 'repair':
            self._repair(message['data'][0], message['data'][1])
        elif message['cmd'] == 'dict':
            self._update_file_dict(message['data'], message['ip'])
        elif message['cmd'] == 'recv':
            self._recv_file_from(message['data'])
            if self._hash(message['data']) == self.local_ip:
                self._repair_put(message['data'])
        elif message['cmd'] == 'del_file':
            self._del_file_by_name(message['data'])
        elif message['cmd'] == 'get_all':
            all_files = self._get_all_versions(message['data'])
            self._unicast('req', len(all_files), message['ip'], 9100 + self.dfs_socket_dict['req'][1], False)
            for f in all_files:
               self._send_file_to(f, message['ip'], self.tcp_port)
    '''
    -----------------------------------------------------------------------
                                Main Function
    -----------------------------------------------------------------------
    '''


if __name__ == '__main__':
    dfsServer = DfsServer()
    if dfsServer.start_join():
        dfsServer.run()
        dfsServer.terminate()
    else:
        print("DFS server not properly configured. Exit")


