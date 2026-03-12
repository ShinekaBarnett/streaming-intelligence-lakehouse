# 04_sessionization.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("Sessionization").getOrCreate()

df = spark.read.format("delta").load("tables/silver_playback_events")

sessions = (
    df.groupBy("session_id")
      .agg(count("event_id").alias("events_per_session"))
)

sessions.write.format("delta") \
    .mode("overwrite") \
    .save("tables/silver_user_sessions")

print("Session table created.")