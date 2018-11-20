import time
import socket
import logging
import os
import pickle as pk
import threading
import select
from env import IP_LIST, MMP_TCP_PORT, DFS_TCP_PORT, CLIENT_TCP_PORT
#import glob


class MmpServer:
    def __init__(self):
        self.membership_list = []
        self.neighbors = []
        self.ip_list = IP_LIST

        self.mmp_receiver = threading.Thread(target=self.mmp_receiver_thread)
        self.mmp_sender = threading.Thread(target=self.mmp_sender_thread)
        self.mmp_tcp_receiver = threading.Thread(target=self.mmp_tcp_receiver_thread)
        self.mmp_cmd = threading.Thread(target=self.mmp_cmd_thread)

        num_mmp_socket = 9
        self.mmp_socket_list = []
        for i in range(num_mmp_socket):
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
            'info': (self.mmp_socket_list[8], 8)}

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

        self.tcp_port = MMP_TCP_PORT
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.listen(10)

        self.leader = None
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.is_running = False
        # -----------
        # Logger Info
        # -----------
        self.logger = logging.getLogger('mmp')
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler('../mmp.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def run(self):
        self.mmp_receiver.start()
        self.mmp_sender.start()
        self.mmp_tcp_receiver.start()
        self.mmp_cmd.start()

    def terminate(self):
        self.mmp_receiver.join()
        self.mmp_sender.join()
        self.mmp_tcp_receiver.join()
        self.mmp_cmd.join()

    def _unicast(self, cmd, msg, ip, port, flag):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        sender_port = 9000 + self.mmp_socket_dict[cmd][1]
        packet = pk.dumps({
            'cmd': cmd,
            'data': msg,
            'ip': self.local_ip,
            'sender_port': sender_port,
            'sender_timestamp': time.time()
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def _multicast(self, cmd, msg, target_list, port, flag):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        skt.settimeout(2)
        sender_port = 9000 + self.mmp_socket_dict[cmd][1]
        packet = pk.dumps({
            'cmd': cmd,
            'data': msg,
            'ip': self.local_ip,
            'sender_port': sender_port,
            'sender_timestamp': time.time()
        })
        for i in target_list:
            skt.sendto(packet, (i, port))
        skt.close()

    def _if_in_mmp(self, ip):
        member_hosts = [i[0] for i in self.membership_list]
        return ip in member_hosts

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
                #for f in self.file_dict.keys():
                #    self._start_repair_file(f, msg['data'][0], 'join')
            else:
                self.membership_list.append(msg['data'])
                self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
            self.logger.info(msg['data'][0] + " is added to the group")
            self._update_neighbors()
            self._elect()
            #if self.local_ip == self.leader:
            #    #self._build_file_dict()
            #    all_affected_files = []
            #    for f in self.file_dict.keys():
            #        all_affected_files.append(f)
            #    for f in all_affected_files:
            #        new_pri = self._hash(f)
            #        self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

    def decommission_request(self, msg):
        if self._if_in_mmp(msg['data'][0]):
            members = [i[0] for i in self.membership_list]
            self.membership_list.pop(members.index(msg['data'][0]))
            self.membership_list.sort(key=lambda i: self.ip_list[i[0]])
            self.logger.info(msg['data'][0] + " left the group")
            self._update_neighbors()
        if not self._if_in_mmp(self.leader):
            self._elect()

        #if self.leader == self.local_ip:
        #    self._start_repair_ip(msg['data'][0])
        #    # TODO no build
        #    all_affected_files = []
        #    for f in self.file_dict.keys():
        #        all_affected_files.append(f)
        #    for f in all_affected_files:
        #        new_pri = self._hash(f)
        #        self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

    def quit_request(self, left_ip):
        member_hosts = [member[0] for member in self.membership_list]
        if left_ip in member_hosts:
            idx = member_hosts.index(left_ip)
            self.membership_list.pop(idx)
            #self.membership_list.sort(key=lambda i:self.ip_list[i[0]])
            self.logger.info(left_ip + " is detected left the group")
        self._multicast('decommission', (left_ip, time.time()),
                        [i[0] for i in self.membership_list if i[0] != self.local_ip],
                        9000 + self.mmp_socket_dict['decommission'][1], True)
        self._update_neighbors()
        if not self._if_in_mmp(self.leader):
            self._elect()

#        if self.local_ip == self.leader:
#            self._start_repair_ip(left_ip)
#            # TODO build
#            all_affected_files = []
#            for f in self.file_dict.keys():
#                all_affected_files.append(f)
#            for f in all_affected_files:
#                new_pri = self._hash(f)
#                self.file_dict[f] = [new_pri] + self._get_neighbors(new_pri)

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

    def mmp_tcp_receiver_thread(self):
        while True:
            try:
                conn, addr = self.tcp_socket.accept()
                print('Connection addr:', addr)
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    print(pk.loads(data))
                mmp = pk.dumps(self.membership_list)
                total_len = len(mmp)
                total_sent = 0
                while total_sent < total_len:
                    sent = conn.send(mmp[total_sent:])
                    if sent == 0:
                        raise RuntimeError("socket connection broken")
                    total_sent = total_sent + sent
                conn.close()
            except socket.timeout:
                continue

    def mmp_cmd_thread(self):
        while True:
            cmd = input('Available cmds: ls, self, join, dec, ld and exit. Enter: ')
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
        elif message['cmd'] == 'leader':
            self.leader = message['ip']
            self._update_neighbors()
            #self._build_file_dict()
            # TODO build file dict

    '''
    -----------------------------------------------------------------------
                                Main Function
    -----------------------------------------------------------------------
    '''


if __name__ == '__main__':
    mmpServer = MmpServer()
    if mmpServer.start_join():
        mmpServer.run()
        mmpServer.terminate()
    else:
        print("mmp server not properly configured. Exit")
