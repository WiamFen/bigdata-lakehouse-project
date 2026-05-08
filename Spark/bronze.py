from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze").getOrCreate()

df = spark.read.csv("/data/sales.csv", header=True)

df.show()