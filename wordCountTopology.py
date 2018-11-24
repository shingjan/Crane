from topology import TopologyBuilder
from bolt import Bolt


class split_bolt:
    def __init__(Bolt):
        super().__init__()

    def execute(self, tup, collector):
        pass


class count_bolt:
    def __init__(Bolt):
        super().__init__()

    def execute(self, tup, collector):
        pass

if __name__ == '__main__':
    topologyBuilder = TopologyBuilder()
    topologyBuilder.set_spout('wordcount.txt')
    topologyBuilder.set_bolt(split_func)
    topologyBuilder.set_bolt(count_func)

