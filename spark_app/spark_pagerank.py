from __future__ import print_function
from __future__ import division
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def transform_to_url_tuple(string):
    urls = string.split('\t')
    return (urls[0], urls[1:])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_pagerank.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingPageRank")
    ssc = StreamingContext(sc, 10)

    lines = ssc.textFileStream(sys.argv[1])
    links = lines.filter(lambda l: len(l.split('\t')) > 1) \
        .map(transform_to_url_tuple) \
        .groupByKey()
    ranks = links.map(lambda (name, neighbours): (name, 0.0))

    contribs = links \
        .join(ranks) \
        .flatMap(
        lambda (name, (neighbours, score)): map(lambda neighbour: (neighbour, 1 / len(neighbours)), neighbours))

    ranks = contribs \
        .reduceByKey(lambda score_by_a, score_by_b: score_by_a + score_by_b) \

    ranks.saveAsTextFiles("pr_output")
    ranks.pprint()

    ssc.start()
    ssc.awaitTermination()
