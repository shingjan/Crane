import random
from util import Bolt, Topology, Tuple, TupleBatch, CRANE_SLAVE_PORT, CRANE_AGGREGATOR_PORT


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        next_node_index = random.randint(0, len(mmp_list) - 1)
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            tup = tup.replace("\n", "")
            words = tup.split(' ')
            for word in words:
                tmp_tuple = Tuple(word)
                xor_id ^= tmp_tuple.uid
                new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid, new_tuple_batch.uid,
                       mmp_list[next_node_index][0], CRANE_SLAVE_PORT)
        collector.ack(rid, xor_id)


class CountBolt(Bolt):
    def __init__(self):
        self.counts = {}
        super(CountBolt, self).__init__('CountBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tuple in tuple_batch.tuple_list:
            word = big_tuple.tup
            count = 0
            if word in self.counts:
                count = self.counts.get(word)
            count += 1
            self.counts[word] = count
        for word, count in self.counts.items():
            tmp_tuple = Tuple((word, count))
            new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num, new_tuple_batch, new_tuple_batch.uid, 0,
                       collector.master, CRANE_AGGREGATOR_PORT)
        collector.ack(rid, xor_id)
        self.counts.clear()


word_count_topology = Topology("WordCount Topology")
word_count_topology.set_spout('app/wordcount2MB.csv')
splitBolt = SplitBolt()
countBolt = CountBolt()
word_count_topology.set_bolt(splitBolt)
word_count_topology.set_bolt(countBolt)

