import socket
import pickle as pk
import time
import os
from dfs.env import IP_LIST, CLIENT_TCP_PORT, DFS_TCP_PORT, UDP_SOCKET, UDP_LS_SOCKET, UDP_RECV_SOCKET, UDP_REQ_SOCKET


class DfsClient:
    def __init__(self):
        self.ip_list = IP_LIST
        self.local_ip = socket.gethostbyname(socket.getfqdn())
        self.tcp_port = CLIENT_TCP_PORT
        self.server_tcp_port = DFS_TCP_PORT
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(('0.0.0.0', UDP_SOCKET))
        self.udp_socket.settimeout(2)
        self.udp_ls_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_ls_socket.bind(('0.0.0.0', UDP_LS_SOCKET))
        self.udp_ls_socket.settimeout(2)
        self.udp_recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_recv_socket.bind(('0.0.0.0', UDP_RECV_SOCKET))
        self.udp_recv_socket.settimeout(2)
        self.udp_req_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_req_socket.bind(('0.0.0.0', UDP_REQ_SOCKET))
        self.udp_req_socket.settimeout(2)
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.bind(('0.0.0.0', self.tcp_port))
        self.tcp_socket.settimeout(2)

    def _udp_unicast(self, cmd, msg, ip, port):
        skt = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = pk.dumps({
            'cmd': cmd,
            'data': msg,
            'ip': self.local_ip,
            'sender_port': 9200,  # no use
            'sender_timestamp': time.time()
        })
        skt.sendto(packet, (ip, port))
        skt.close()

    def run(self):
        while True:
            print('Available cmds: ')
            print('1. put localfilename sdfsfilename')
            print('2. get sdfsfilename localfilename')
            print('3. delete sdfsfilename')
            print('4. ls sdfsfilename')
            print('5. get-versions sdfsfilename numversions localfilename')
            cmd = input('Enter: ')
            cmd_token = cmd.split(" ")
            self.exec_dfs_cmd(cmd_token)

    def _get_leader(self):
        for i in self.ip_list.keys():
            self._udp_unicast('ask', '', i, 9001)
        try:
            message, addr = self.udp_socket.recvfrom(65536)
            msg = pk.loads(message)
            return msg['data']
        except socket.timeout:
            print("Cluster is not available currently. Try again")

    def _send_file_to(self, local_name, ip, port):
        target = open(local_name, "rb")
        skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connected = False
        while not connected:
            try:
                skt.connect((ip, port))
                connected = True
            except:
                pass
        while True:
            chunk = target.read(1024)
            if not chunk:
                break  # EOF
            skt.sendall(chunk)
        skt.shutdown(socket.SHUT_RDWR)
        target.close()

    def _recv_file_from(self, file_name):
        self.tcp_socket.listen(1)
        connected = False
        print("tcp socket is listening to")
        while not connected:
            try:
                client, addr = self.tcp_socket.accept()
                connected = True
            except socket.timeout:
                pass
        f = open(file_name, 'wb')
        while True:
            content = client.recv(1024)
            if not content:
                break  # EOF
            f.write(content)
        print("File well received.")
        f.close()

    def exec_dfs_cmd(self, cmd_token):
        """
        put localfilename sdfsfilename:
        get sdfsfilename localfilename:
        delete sdfsfilename:
        ls sdfsfilename: list all machine (VM) addresses where this file is currently
            being stored
        store:  At any machine, list all files currently being stored at this
            machine.
        get-versions sdfsfilename numversions localfilename: which gets all the last num-versions
            versions of the file into the localfilename (use delimiters to mark out versions).
        :param cmd_token:
        :return:
        """
        cmd = cmd_token[0]
        if cmd == "exit":
            os._exit(0)
        param_len = len(cmd_token) - 1
        leader = self._get_leader()
        print("Talking to leader: " + leader + " on cmd: " + cmd)
        start = time.time()
        if cmd == "put" and param_len == 2:
            local_name = cmd_token[1]
            sdfs_name = cmd_token[2]
            self._udp_unicast('put', sdfs_name, leader, 9101)
            try:
                message, addr = self.udp_req_socket.recvfrom(65535)
                msg = pk.loads(message)
                print('primary node is', msg['data'])
                self._udp_unicast('recv', sdfs_name, msg['data'], 9108)
                self._send_file_to(local_name, msg['data'], self.server_tcp_port)
            except socket.timeout:
                print("No recv from leader. Abort")
        elif cmd == "get" and param_len == 2:
            sdfs_name = cmd_token[1]
            local_name = cmd_token[2]
            self._udp_unicast('get', sdfs_name, leader, 9100)
            try:
                message, addr = self.udp_recv_socket.recvfrom(65535)
                msg = pk.loads(message)
                if msg['data'] != leader:
                    self._udp_unicast('get', sdfs_name, msg['data'], 9100)
                self._recv_file_from(local_name)
            except socket.timeout:
                print("No recv from leader. Abort")
        elif cmd == "delete" and param_len == 1:
            sdfs_name = cmd_token[1]
            self._udp_unicast('del', sdfs_name, leader, 9102)
        elif cmd == "ls" and param_len == 1:
            sdfs_name = cmd_token[1]
            self._udp_unicast('ls', sdfs_name, leader, 9103)
            try:
                message, addr = self.udp_ls_socket.recvfrom(65536)
                msg = pk.loads(message)
                print(msg['data'])
            except socket.timeout:
                print("No ls info from leader. Abort.")
        elif cmd == "get-versions" and param_len == 3:
            sdfs_name = cmd_token[1]
            num_versions = cmd_token[2]
            local_name = cmd_token[3]
            self._udp_unicast('getv', (sdfs_name, num_versions), leader, 9104)
            try:
                message, addr = self.udp_req_socket.recvfrom(65535)
                msg = pk.loads(message)
                print('primary node is', msg['data'])
                self._udp_unicast('getv', (sdfs_name, num_versions), msg['data'], 9104)
                self._recv_file_from(local_name)
            except socket.timeout:
                print('No get-versions info from leader. Abort.')
        else:
            print("Invalid cmd or params, enter again:")
        print("Time usage: ", time.time() - start)


if __name__ == '__main__':
    dfsClient = DfsClient()
    dfsClient.run()

