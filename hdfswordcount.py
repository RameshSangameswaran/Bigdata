from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

sc = spark.sparkContext

# Read text file
text_file = sc.textFile("hdfs:///home/hadoop/wordcount/input.txt")

# Word Count Logic
counts = (text_file
          .flatMap(lambda line: line.split())
          .map(lambda word: (word.lower(), 1))
          .reduceByKey(lambda a, b: a + b))

# Save output
counts.saveAsTextFile("hdfs:///home/hadoop/wordcount/output")

# Display output
for item in counts.collect():
    print(item)

spark.stop()
