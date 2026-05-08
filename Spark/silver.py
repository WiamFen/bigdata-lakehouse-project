from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver").getOrCreate()

df = spark.read.csv("/data/sales.csv", header=True)

df_clean = df.dropna()

df_clean.show()