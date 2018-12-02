from __future__ import print_function
from __future__ import division
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingPageRank")
    ssc = StreamingContext(sc, 10)

    lines = ssc.textFileStream(sys.argv[1])

    def computeContribs(urls, length):
        """Calculates URL contributions to the rank of other URLs."""
        for u in urls:
            yield (u, 1/length)

    def combine_list(new_list, old_list):
        re = []
        for iterable in new_list:
            for item in iterable:
                re.append(item)
        if old_list is None:
            return re
        s = set(re)
        for iterable in old_list:
            for item in iterable:
                s.add(item)
        return list(s)

    lines = lines.filter(lambda l: len(l.split('\t')) > 1)
    ranks = lines.map(lambda l: 1/len(l.split('\t')[1: ]))
    links = lines.map(lambda l: l.split('\t')[1: ])

    counts = links.join(ranks).groupByKey().updateStateByKey(combine_list)

    counts = counts.reduceByKey(lambda a, b: a+b)

    counts.saveAsTextFiles("pr_output")
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
