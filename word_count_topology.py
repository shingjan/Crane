from util import Bolt, Topology, Tuple


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, rid, tup, collector):
        words = tup.split(' ')
        xor_id = rid
        for word in words:
            print(word)
            tmp_tuple = Tuple(word)
            xor_id ^= tmp_tuple.uid
            collector.emit(tmp_tuple)
        collector.ack(tup, rid, rid)  # xor_id)


class CountBolt(Bolt):
    def __init__(self):
        super(CountBolt, self).__init__('CountBolt')
        self.counts = {}

    def execute(self, rid, tup, collector):
        word = tup[0]
        count = 0
        if word in self.counts:
            count = self.counts.get(word)
        count += 1
        self.counts[word] = count
        tmp_tuple = Tuple((word, count))
        collector.emit(tmp_tuple)
        collector.ack(tup, rid, rid ^ tmp_tuple.uid)


word_count_topology = Topology("WordCount Topology")
word_count_topology.set_spout('README.md')
word_count_topology.set_bolt(SplitBolt, 'shuffle')
word_count_topology.set_bolt(CountBolt, 'hash')

