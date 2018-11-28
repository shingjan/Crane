from util import Bolt, Topology, Tuple, TupleBatch, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector):
        new_tuple_batch = TupleBatch()
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            tup = tup.replace("\n", "")
            words = tup.split(' ')
            for word in words:
                tmp_tuple = Tuple(word)
                xor_id ^= tmp_tuple.uid
                new_tuple_batch.add_tuple(tmp_tuple)
                # TODO: Change the hard-code next-bolt receiver to a hashed one
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid, new_tuple_batch.uid, "172.22.154.210",
                       CRANE_SLAVE_UDP_PORT)
        collector.ack(rid, xor_id)


class CountBolt(Bolt):
    def __init__(self):
        self.counts = {}
        super(CountBolt, self).__init__('CountBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector):
        new_tuple_batch = TupleBatch()
        for big_tuple in tuple_batch.tuple_list:
            word = big_tuple.tup
            count = 0
            if word in self.counts:
                count = self.counts.get(word)
            count += 1
            self.counts[word] = count
            tmp_tuple = Tuple((word, count))
            new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num, new_tuple_batch, new_tuple_batch.uid, 0, "172.22.154.209", CRANE_AGGREGATOR_PORT)
        collector.ack(rid, xor_id)


word_count_topology = Topology("WordCount Topology")
word_count_topology.set_spout('app/wordcount50.csv')
splitBolt = SplitBolt()
countBolt = CountBolt()
word_count_topology.set_bolt(splitBolt, 'shuffle')
word_count_topology.set_bolt(countBolt, 'hash')

