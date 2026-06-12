from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDDDemo") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD
rdd = sc.textFile("file:///home/hadoop/sparklab/data.txt")

# Lazy Transformations
numbers = rdd.map(lambda x: int(x))
squared = numbers.map(lambda x: x*x)

print("Transformation defined")

# Action triggers execution
result = squared.collect()

print("Squared Values:")
print(result)

# Save RDD
squared.saveAsTextFile("file:///home/hadoop/sparklab/output")

spark.stop()
