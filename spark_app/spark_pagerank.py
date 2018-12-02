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
    #lines = lines.flatMap(lambda line: [(i, 1/len(line.split(",")[1: ])) for i in line.split(",")[1: ]])
    temp = lines.map(lambda line: 1/len(line.split('\t')[1: ]))
    lines = lines.map(lambda line: line.split('\t')[1: ])

    counts = lines.join(temp).reduceByKey(lambda a, b: a+b)
    counts.saveAsTextFiles("pr_output")
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
