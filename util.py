class Bolt:
    def __init__(self, bolt_type):
        self.next_bolt = None
        self.bolt_type = bolt_type

    def execute(self, tup, collector):
        pass


class TopologyBuilder:
    def __init__(self):
        self.bolt_graph = None

    def set_bolt(self, *args):
        pass

    def set_spout(self, file_path):
        pass

