from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonStreamingTT")
    ssc = StreamingContext(sc, 10)

    lines = ssc.textFileStream(sys.argv[1])
    counts = lines.flatMap(lambda line: line.split(","))\
                 .filter(lambda x: x.isdigit() and x > 50)\
                 .map(lambda x: ('result', 1))\
                 .reduceByKey(lambda a, b: a+b)

    counts.saveAsTextFiles("tt_output")
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
