from util import Bolt, Topology, Tuple, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tup, collector):
        words = tup.split(' ')
        for word in words:
            tmp_tuple = Tuple(word)
            xor_id ^= tmp_tuple.uid
            # TODO: Change the hard-code next-bolt receiver to a hashed one
            collector.emit(top_num, bolt_num + 1, tmp_tuple, rid, tmp_tuple.uid, "172.22.154.210", CRANE_SLAVE_UDP_PORT)
        collector.ack(rid, xor_id)


class CountBolt(Bolt):
    def __init__(self):
        self.counts = {}
        super(CountBolt, self).__init__('CountBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tup, collector):
        word = tup.tup
        count = 0
        if word in self.counts:
            count = self.counts.get(word)
        count += 1
        self.counts[word] = count
        tmp_tuple = Tuple((word, count))
        collector.emit(top_num, bolt_num, tmp_tuple, tmp_tuple.uid, 0, "172.22.154.209", CRANE_AGGREGATOR_PORT)
        collector.ack(rid, xor_id)


class AggregateBolt(Bolt):
    def __init__(self):
        super(AggregateBolt, self).__init__('CountBolt')
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
word_count_topology.set_spout('wordcount50.csv')
splitBolt = SplitBolt()
countBolt = CountBolt()
word_count_topology.set_bolt(splitBolt, 'shuffle')
word_count_topology.set_bolt(countBolt, 'hash')

