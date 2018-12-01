import uuid


class Bolt:
    def __init__(self, bolt_type):
        self.next_bolt = None
        self.bolt_type = bolt_type

    def execute(self, top_num, bolt_num, rid, tup, collector, mmp_list):
        pass


class Spout:
    def __init__(self, file_path):
        self.target = open(file_path, "r")

    def next_tup(self):
        print('next tup is called')
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

    def set_bolt(self, bolt):
        self.bolt_list.append(bolt)

    def set_spout(self, file_path):
        self.spout = Spout(file_path)


class Tuple:
    def __init__(self, tup):
        self.tup = tup
        self.uid = uuid.uuid4().int


class TupleBatch:
    def __init__(self):
        self.tuple_list = []
        self.uid = 0

    def add_tuple(self, big_tuple):
        self.tuple_list.append(big_tuple)
        self.uid ^= big_tuple.uid


CRANE_AGGREGATOR_PORT = 9527
CRANE_SLAVE_PORT = 9528
CRANE_MAX_INTERVAL = 30
CRANE_BATCH_SIZE = 8000

