# 02_bronze_ingestion.py
# Load raw playback events into Bronze Delta table

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

df = spark.read.option("header", True).csv("data/generated/playback_events.csv")

df.write.format("delta") \
    .mode("overwrite") \
    .save("tables/bronze_playback_events")

print("Bronze ingestion complete.")