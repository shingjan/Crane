from __future__ import print_function
from __future__ import division
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 10)

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line: [(i, 1/len(line.split(",")[1: ])) for i in line.split(",")[1: ]])\
            .map(lambda x: (x[0], x[1]))\
            .reduceByKey(lambda a, b: a+b)

    counts.saveAsTextFiles("wordcount_output")
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
