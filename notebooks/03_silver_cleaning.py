# 03_silver_cleaning.py
# Clean playback events

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("SilverCleaning").getOrCreate()

df = spark.read.format("delta").load("tables/bronze_playback_events")

clean_df = (
    df.dropDuplicates(["event_id"])
      .filter(col("watch_seconds") >= 0)
)

clean_df.write.format("delta") \
    .mode("overwrite") \
    .save("tables/silver_playback_events")

print("Silver layer created.")