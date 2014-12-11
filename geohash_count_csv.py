__author__ = 'samchorlton'

from pyspark import SparkContext

sc = SparkContext("local", "geohash_count_csv")

inputfile = sc.textFile("/Users/samchorlton/Downloads/query_result(5).csv")

counts = inputfile.map(lambda line: line.split(",")) \
             .map(lambda line: (line[1], 1)) \
             .reduceByKey(lambda a, b: a + b)

counts.saveAsTextFile("/Users/samchorlton/Documents/Spark/output_geohash.csv")