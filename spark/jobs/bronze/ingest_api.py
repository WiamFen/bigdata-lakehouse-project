# ingest_api.py
import requests
import json
from spark_session import get_spark

spark = get_spark()

# FakeStore API
res1 = requests.get("https://fakestoreapi.com/products").json()
with open("data/raw/api/fakestore_products.json", "w") as f:
    json.dump(res1, f)

df1 = spark.read.json("data/raw/api/fakestore_products.json")

# DummyJSON API
res2 = requests.get("https://dummyjson.com/products").json()["products"]
with open("data/raw/api/dummyjson_products.json", "w") as f:
    json.dump(res2, f)

df2 = spark.read.json("data/raw/api/dummyjson_products.json")

df1.write.mode("overwrite").saveAsTable("bronze_fakestore")
df2.write.mode("overwrite").saveAsTable("bronze_dummyjson")