from util import Bolt, Topology, Tuple, TupleBatch


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            tup = tup.replace("\n", "")
            words = tup.split(' ')
            for word in words:
                tmp_tuple = Tuple(word)
                new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid)


class CountBolt(Bolt):
    def __init__(self):
        self.counts = {}
        super(CountBolt, self).__init__('CountBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
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
        collector.ack(top_num, bolt_num, new_tuple_batch, rid)
        self.counts.clear()


word_count_topology = Topology("WordCount Topology")
word_count_topology.set_spout('app/wordcount700b.csv')
splitBolt = SplitBolt()
countBolt = CountBolt()
word_count_topology.set_bolt(splitBolt)
word_count_topology.set_bolt(countBolt)

