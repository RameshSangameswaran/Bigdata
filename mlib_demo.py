from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder \
    .appName("MLlibDemo") \
    .getOrCreate()

data = spark.read.csv(
    "file:///home/hadoop/sparklab/salary.csv",
    header=True,
    inferSchema=True
)

assembler = VectorAssembler(
    inputCols=["experience"],
    outputCol="features"
)

dataset = assembler.transform(data)

lr = LinearRegression(
    featuresCol="features",
    labelCol="salary"
)

model = lr.fit(dataset)

predictions = model.transform(dataset)

predictions.select(
    "experience",
    "salary",
    "prediction"
).show()

spark.stop()
