from util import Bolt, TopologyBuilder


class SplitBolt(Bolt):
    def __init__(self):
        super(SplitBolt, self).__init__('SplitBolt')

    def execute(self, tup, collector):
        words = tup.values[0].split(' ')
        for word in words:
            collector.emit([word])


class CountBolt(Bolt):
    def __init__(self):
        super(CountBolt, self).__init__('CountBolt')
        self.counts = {}

    def execute(self, tup, collector):
        word = tup.getString(0)
        count = 0
        if word in self.counts:
            count = self.counts.get(word)
        count += 1
        self.counts[word] = count
        collector.emit((word, count))


if __name__ == '__main__':
    topologyBuilder = TopologyBuilder()
    topologyBuilder.set_spout('wordcount.txt')
    topologyBuilder.set_bolt(SplitBolt)
    topologyBuilder.set_bolt(CountBolt)

