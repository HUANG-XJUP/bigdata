import time

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Cosine Similarity") \
    .getOrCreate()

data = [("Document1", "This is a sentence."), ("Document2", "This is another sentence."), ("Document3", "This is yet another sentence.")]
df = spark.createDataFrame(data, ["DocumentID", "Text"])

print(time.strftime("%Y-%m-%d %H:%M:%S"))
