__author__ = 'samchorlton'

from pyspark import SparkContext

def splitLine(line):
    return line.split(",")

def pullHash(line):
    return (line[1], 1)

def countHashes(hash1Count, hash2Count):
    return hash1Count + hash2Count

sc = SparkContext("local", "geohash_count_csv")

inputfile = sc.textFile("/Users/samchorlton/Downloads/query_result(5).csv")

counts = inputfile.map(splitLine) \
             .map(pullHash) \
             .reduceByKey(countHashes)

counts.saveAsTextFile("/Users/samchorlton/Documents/Spark/output_geohash_2.csv")