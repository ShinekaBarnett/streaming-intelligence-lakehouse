# 05_gold_metrics.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("GoldMetrics").getOrCreate()

df = spark.read.format("delta").load("tables/silver_playback_events")

daily_views = (
    df.groupBy("content_id")
      .agg(count("*").alias("total_views"))
)

daily_views.write.format("delta") \
    .mode("overwrite") \
    .save("tables/gold_content_views")

print("Gold metrics created.")

