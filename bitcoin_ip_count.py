"""
 Counts words in text encoded with UTF8 received from the network every second.

 Usage: recoverable_network_wordcount.py <hostname> <port> <checkpoint-directory> <output-file>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
   data. <checkpoint-directory> directory to HDFS-compatible file system which checkpoint data
   <output-file> file to which the word counts will be appended

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`

 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/recoverable_network_wordcount.py \
        localhost 9999 ~/checkpoint/ ~/out`

 If the directory ~/checkpoint/ does not exist (e.g. running for the first time), it will create
 a new StreamingContext (will print "Creating new context" to the console). Otherwise, if
 checkpoint data exists in ~/checkpoint/, then it will create StreamingContext from
 the checkpoint data.
"""

import os
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def splitLine(line):
    return line.split(",")

def createContext(host, port, outputPath):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint
    print "Creating new context"
    if os.path.exists(outputPath):
        os.remove(outputPath)
    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 120)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, port)
    print '\n\n\nconnectionMade\n\n\n'
    addresses = lines.map(splitLine)
    transcationsum = addresses.map(lambda x: (x[0], (1, x[1]))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    def echo(time, rdd):
        counts = "Counts at time %s %s" % (time, rdd.collect())
        print counts
        print "Appending to " + os.path.abspath(outputPath)
        with open(outputPath, 'a') as f:
            f.write(counts + "\n")

    transcationsum.foreachRDD(echo)
    return ssc

if __name__ == "__main__":
    ssc = createContext('localhost', int(2227), 'outputSam')
    ssc.start()
    ssc.awaitTermination()
