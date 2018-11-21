import time
import socket
import logging
import os
import pickle as pk
import threading
import select
import glob
#from mmpServer import MmpServer
from env import IP_LIST, INDEX_LIST, DFS_TCP_PORT, CLIENT_TCP_PORT, MMP_TCP_PORT


# TODO: build, delete file
class DfsServer:
    def __init__(self):
        # ----------------------------
        # membership server params
        # ----------------------------
        self.membership_list = []
        self.neighbors = []
        self.ip_list = IP_LIST
        self.index_list = INDEX_LIST

        self.leader = None
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.is_running = False
        self.dfs_receiver = threading.Thread(target=self.dfs_receiver_thread)
        self.mmp_receiver = threading.Thread(target=self.mmp_receiver_thread)
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
        self.tcp_port = DFS_TCP_PORT
        self.client_tcp_port = CLIENT_TCP_PORT
        self.mmp_tcp_port = MMP_TCP_PORT
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.settimeout(2)
        self.tcp_socket.listen(10)

    '''
    -----------------------------------------------------------------------
                              Mmp Helper functions
    -----------------------------------------------------------------------
    '''
    def run(self):
        self.dfs_receiver.start()
        self.mmp_receiver.start()
        self.cmd.start()

    def terminate(self):
        self.dfs_receiver.join()
        self.mmp_receiver.join()
        self.cmd.join()

    def _unicast(self, cmd, msg, ip, port, flag):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
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

    def start(self):
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        skt.settimeout(2)
        connected = False
        while not connected:
            try:
                skt.connect(('0.0.0.0', self.mmp_tcp_port))
                connected = True
                print('connection established')
            except socket.timeout:
                pass
        msg = pk.dumps('mmp')
        skt.sendall(msg)
        chunks = []
        while True:
            try:
                data = skt.recv(1024)
                if not data:
                    break
                chunks.append(data)
            except socket.timeout:
                continue
        skt.close()
        return self._delta(pk.loads(b''.join(chunks)))

    def _delta(self, new_mmp):
        self.leader = new_mmp[0]
        if not self.membership_list:
            self.membership_list = new_mmp
        # TODO: return False?
        elif self.membership_list != new_mmp:
            ori_ls = self.membership_list
            self.membership_list = new_mmp
            if self.local_ip == self.leader:
                for f in self.file_dict.keys():
                    self._start_repair_file(f, ori_ls)
        return True

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
        """
        return the nbs of a given ip
        default of ls: current mmp list
        """
        if not ls:
            ls = [i[0] for i in self.membership_list]
            ls.sort(key=lambda i: self.ip_list[i])

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
        return: primary node base on the mmp list input.
        default: current primary node
        """
        if not ls:
            ls = [i[0] for i in self.membership_list]

        s = 0
        for i in filename:
            s += ord(i)
        s = 1 + (s % 10)

        """
        k + len(self.membership_list)%10 = s + 1
        """
        n = len(ls)
        candidates = [s-n-10, s-n, s-n+10]
        for k in candidates:
            if k >= -n and k < n:
                return ls[k]

    def _start_repair_file(self, sdfs_name, ori_ls):
        """
        leader repair
        p: primary node
        op: the last primary node
        ls: current mmp list
        ori_ls: mmp list before change
        """
        p = self._hash(sdfs_name)
        nb = self._get_neighbors(sdfs_name)
        should_in = [p] + nb

        op = self._hash(sdfs_name, ori_ls)
        ori_nb = self._get_neighbors(sdfs_name, ori_ls)
        was_in = [op] + ori_nb
        to_del = [i for i in was_in if i not in should_in]
        to_get = [i for i in should_in if i not in was_in]

        alive = [i[0] for i in self.membership_list]
        candidates = [i for i in was_in if i in alive]
        absoluate_candidates = [i for i in candidates if i in should_in]
        if not absoluate_candidates:
            sender = candidates[0]
        else:
            sender = absoluate_candidates[0]

        for n in to_get:
            self._send_file_from_to(sdfs_name, sender, n)

        for n in to_del:
            self._del_file_from(sdfs_name, n)

    def _del_file_from(sdfs_name, node_ip):
        # TODO
        """
        sdfs_name: file to del
        node_ip: node should del the file
        """
        pass

    def _send_file_from_to(sdfs_name, sender_ip, receiver_ip):
        """
        sdfs_name: file to send/recv
        sender_ip: the primary
        """
        pass

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

    def mmp_receiver_thread(self):
        while True:
            time.sleep(1)
            self.start()

    def cmd_thread(self):
        while True:
            cmd = input('Available cmds: ls, self, join, dec, store, ld and exit. Enter: ')
            if cmd == 'join':
                pass
                #if not self.start_join():
                #    print("Rejoin failed. Try rejoin again:")
            elif cmd == 'dec':
                pass
                #self.decommission()
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

    '''
    -----------------------------------------------------------------------
                                Main Function
    -----------------------------------------------------------------------
    '''


if __name__ == '__main__':
    #mmpServer = MmpServer()
    dfsServer = DfsServer()
    if dfsServer.start():
        dfsServer.run()
        dfsServer.terminate()
    else:
        print("dfsServer not configured properly. Abort!")



