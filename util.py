class Bolt:
    def __init__(self, bolt_type):
        self.next_bolt = None
        self.bolt_type = bolt_type

    def execute(self, tup, collector):
        pass


class Spout:
    def __init__(self, file_path):
        self.target = open(file_path, "r")

    def next_tup(self):
        line = self.target.readline()
        if not line:
            self.close()
            return None
        return line


    def close(self):
        self.target.close()


class Topology:
    def __init__(self, name):
        self.name = name
        self.bolt_list = []
        self.spout = None

    def set_bolt(self, bolt, group_methd):
        self.bolt_list.append(bolt)

    def set_spout(self, file_path):
        self.spout = Spout(file_path)


CRANE_MASTER_UDP_PORT = 9527
CRANE_SLAVE_UDP_PORT = 9528

IP_LIST = {
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
