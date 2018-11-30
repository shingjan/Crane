import random
from util import Bolt, Topology, Tuple, TupleBatch, CRANE_SLAVE_UDP_PORT, CRANE_AGGREGATOR_PORT

class ParseNeighborsBolt(Bolt):
    def __init__(self):
        super(ParseNeighborsBolt, self).__init__('ParseNeighborsBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        next_node_index = random.randint(0, len(mmp_list) - 1)
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            tup = tup.replace("\n", "")
            url_list = tup.split('\t')
            urls = [url_list[i] for i in range(len(url_list)) if i != 0]
            weight = len(urls)
            for url in urls:
                tmp_tuple = Tuple((url, 1/weight))
                xor_id ^= tmp_tuple.uid
                new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid, new_tuple_batch.uid,
                       mmp_list[next_node_index][0], CRANE_SLAVE_UDP_PORT)
        collector.ack(rid, xor_id)


class ComputeContribsBolt(Bolt):
    def __init__(self):
        self.ranks = {}
        super(ComputeContribsBolt, self).__init__('ComputeContribsBolt')

    def execute(self, top_num, bolt_num, rid, xor_id, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tup in tuple_batch.tuple_list:
            print(big_tup.tup)
            url, rank = big_tup.tup
            if url in self.ranks:
                rank += self.ranks.get(url)
            self.ranks[url] = rank
        for url, rank in self.ranks.items():
            tmp_tuple = Tuple((url, rank))
            new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num, new_tuple_batch, new_tuple_batch.uid, 0,
                       collector.master, CRANE_AGGREGATOR_PORT)
        collector.ack(rid, xor_id)
        self.ranks.clear()


page_rank_topology = Topology("PageRank Topology")
page_rank_topology.set_spout('app/page_rank.csv')
computeContribsBolt = ComputeContribsBolt()
parseNeighborsBolt = ParseNeighborsBolt()
page_rank_topology.set_bolt(computeContribsBolt)
page_rank_topology.set_bolt(parseNeighborsBolt)
