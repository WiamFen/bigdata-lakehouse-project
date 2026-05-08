from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gold").getOrCreate()

df = spark.read.csv("/data/sales.csv", header=True)

df.createOrReplaceTempView("sales")

result = spark.sql("""
SELECT channel, SUM(total_amount) as revenue
FROM sales
GROUP BY channel
""")

result.show()