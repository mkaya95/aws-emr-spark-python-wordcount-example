import sys
from operator import add
from pyspark import SparkConf, SparkContext
import re

conf = (SparkConf()
		 .setMaster("local")
		 .setAppName("WordCounter")
		 .set("spark.executor.memory", "1g"))
sc = SparkContext(conf = conf)

print("Launch App..")
if __name__ == "__main__":
	print("Initiating main..")
	
	inputFile = "s3://bigdatatechnologies/spark-emr/"
	print("Counting words in ", inputFile)
	lines = sc.wholeTextFiles(inputFile)
	
	lines_nonempty = lines.filter( lambda x: len(x) > 0 )
	# Since we are not taking care about numbers and other chars then we are just greping by words
	counts = lines_nonempty.flatMapValues(lambda x: re.findall("([a-zA-Z]{3,})", x)) \
				  .map(lambda x: ((x[0], x[1]), 1) ) \
				  .reduceByKey(add)
	output = counts.collect()
	
	for (word, count) in output:
		print("%s:%s:%i" % (word[1], word[0].split('/')[-1].split('.')[0], count))
		# output format be word:exam:count
	sc.stop()