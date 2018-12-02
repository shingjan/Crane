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

    def computeContribs(urls):
        """Calculates URL contributions to the rank of other URLs."""
        num_urls = len(urls)
        for url in urls:
            yield (url, 1/num_urls)

    lines = lines.filter(lambda l: len(l.split('\t')) > 1)
    #ranks = lines.map(lambda l: 1)
    links = lines.map(lambda l: l.split('\t')[1: ])

    counts = links.flatMap(lambda x: computeContribs(x))
    counta = counts.reduceByKey(lambda a, b: a+b)
    counts.saveAsTextFiles("pr_output")
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
