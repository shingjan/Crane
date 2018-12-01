import random
from util import Bolt, Topology, Tuple, TupleBatch, CRANE_SLAVE_PORT, CRANE_AGGREGATOR_PORT


class FilterBolt(Bolt):
    def __init__(self):
        super(FilterBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        next_node_index = random.randint(1, len(mmp_list) - 1)
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            tup = tup.replace("\n", "")
            words = tup.split(',')
            if int(words[1]) > 50:
                tmp_tuple = Tuple((words[0], 1))
                new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid,  mmp_list[next_node_index][0],
                       CRANE_SLAVE_PORT)

class CountBolt(Bolt):
    def __init__(self):
        self.counter = 0
        super(CountBolt, self).__init__('CountBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tuple in tuple_batch.tuple_list:
            word = big_tuple.tup
            self.counter += word[1]
        tmp_tuple = Tuple(('result', self.counter))
        new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num, new_tuple_batch, rid, collector.master, CRANE_AGGREGATOR_PORT)
        self.counter = 0

twitter_user_filter_topology = Topology("third")
twitter_user_filter_topology.set_spout('app/third_80.txt')
filterBolt = FilterBolt()
countBolt = CountBolt()
twitter_user_filter_topology.set_bolt(filterBolt)
twitter_user_filter_topology.set_bolt(countBolt)
