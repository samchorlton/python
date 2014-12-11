__author__ = 'samchorlton'

from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

inputfile = sc.textFile("/Users/samchorlton/Documents/Spark/spark-master/README.md")

counts = inputfile.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("/Users/samchorlton/Documents/Spark/output.csv")