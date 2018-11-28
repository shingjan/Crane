from util import Bolt, Topology, Tuple, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT


class FilterBolt(Bolt):
    def __init__(self):
        super(FilterBolt, self).__init__('SplitBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tup, collector):
        tup = tup.replace("\n", "")
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


twitter_user_filter_topology = Topology("Twitter User Filter Topology")
twitter_user_filter_topology.set_spout('app/twitteruser50.csv')
filterBolt = FilterBolt()
countBolt = CountBolt()
twitter_user_filter_topology.set_bolt(filterBolt, 'shuffle')
twitter_user_filter_topology.set_bolt(countBolt, 'hash')
