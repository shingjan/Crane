from __future__ import print_function
from __future__ import division
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_pagerank.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingPageRank")
    ssc = StreamingContext(sc, 10)
    # 1 2 3 4; 2 3 5;
    lines = ssc.textFileStream(sys.argv[1])
    links = lines.filter(lambda l: len(l.split('\t')) > 1) \
        .map(lambda line: line.split("\t")[1:])
    # [[2,3,4]; [3,5]]
    contribs = links.flatMap(lambda neighbors: map(lambda neighbor: (neighbor, 1 / len(neighbors - 1)), neighbors))
    ranks = contribs.reduceByKey(lambda score_by_a, score_by_b: score_by_a + score_by_b)

    ranks.saveAsTextFiles("pr_output")
    ranks.pprint()

    ssc.start()
    ssc.awaitTermination()
