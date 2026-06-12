from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkJob").getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize(range(1,101))

even = rdd.filter(lambda x: x % 2 == 0)

count = even.count()

print("Even Numbers:", count)

spark.stop()
