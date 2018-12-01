from util import Bolt, Topology, Tuple, TupleBatch


class ParseNeighborsBolt(Bolt):
    def __init__(self):
        super(ParseNeighborsBolt, self).__init__('ParseNeighborsBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tup in tuple_batch.tuple_list:
            tup = big_tup.tup
            #print(tup)
            tup = tup.replace("\n", "")
            url_list = tup.split('\t')
            urls = [url_list[i] for i in range(len(url_list)) if i != 0]
            weight = len(urls)
            for url in urls:
                tmp_tuple = Tuple((url, 1/weight))
                new_tuple_batch.add_tuple(tmp_tuple)
        collector.emit(top_num, bolt_num + 1, new_tuple_batch, rid)


class ComputeContribsBolt(Bolt):
    def __init__(self):
        self.ranks = {}
        super(ComputeContribsBolt, self).__init__('ComputeContribsBolt')

    def execute(self, top_num, bolt_num, rid, tuple_batch, collector, mmp_list):
        new_tuple_batch = TupleBatch()
        for big_tup in tuple_batch.tuple_list:
            url, rank = big_tup.tup
            if url in self.ranks:
                rank += self.ranks.get(url)
            self.ranks[url] = rank
        for url, rank in self.ranks.items():
            tmp_tuple = Tuple((url, rank))
            new_tuple_batch.add_tuple(tmp_tuple)
        collector.ack(top_num, bolt_num, new_tuple_batch, rid)
        self.ranks.clear()


page_rank_topology = Topology("PageRank Topology")
page_rank_topology.set_spout('app/page_rank.csv')
computeContribsBolt = ComputeContribsBolt()
parseNeighborsBolt = ParseNeighborsBolt()
page_rank_topology.set_bolt(parseNeighborsBolt)
page_rank_topology.set_bolt(computeContribsBolt)
