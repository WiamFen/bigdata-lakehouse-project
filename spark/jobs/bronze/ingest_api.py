import sys
sys.path.append("/opt/spark-jobs")

import requests
from utils.spark_session import get_spark_session

spark = get_spark_session()

res1 = requests.get("https://fakestoreapi.com/products").json()
res2 = requests.get("https://dummyjson.com/products").json()["products"]

df1 = spark.read.json(spark.sparkContext.parallelize(res1))
df2 = spark.read.json(spark.sparkContext.parallelize(res2))

df1.writeTo("lakehouse.bronze.bronze_fakestore").createOrReplace()
df2.writeTo("lakehouse.bronze.bronze_dummyjson").createOrReplace()

print("API ingestion DONE")