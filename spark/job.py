from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, to_timestamp, date_format,
    count as F_count, avg as F_avg, max as F_max
)
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Spark session
spark = (
    SparkSession.builder.appName("EarthquakesSparkJob")
    .config("spark.es.nodes", "elasticsearch")       # Elasticsearch hostname in Docker network
    .config("spark.es.port", "9200")
    .config("spark.es.nodes.wan.only", "true")       # often needed in Docker
    .getOrCreate()
)

logging.info("-" * 100)
logging.info("STARTING EARTHQUAKES SPARK JOB")
logging.info("-" * 100)
logging.info("Spark session initialized")

# Read earthquakes data from Elasticsearch
# Index name must match what Logstash writes (recommended: "earthquakes")
df = (
    spark.read.format("org.elasticsearch.spark.sql")
    .option("es.resource", "earthquakes")
    .load()
)

logging.info(f"Data read from Elasticsearch: {df.count()} rows")

# ---- 1) Ensure time field is timestamp (for daily aggregations) ----
# Our producer sends time_iso like "2026-02-27T10:20:30Z"
# to_timestamp can parse ISO; sometimes the "Z" needs stripping.
df_time = df.withColumn(
    "event_ts",
    to_timestamp(regexp_replace(col("time_iso"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS")
)

# If your time_iso has no milliseconds, Spark might need another pattern.
# Alternative safe approach: let Spark infer:
# df_time = df.withColumn("event_ts", to_timestamp(regexp_replace(col("time_iso"), "Z$", "")))

df_time = df_time.filter(col("event_ts").isNotNull())

# Add day column
df_time = df_time.withColumn("event_day", date_format(col("event_ts"), "yyyy-MM-dd"))

# ---- 2) Daily statistics: count, avg magnitude, max magnitude ----
daily_stats = (
    df_time.groupBy("event_day")
    .agg(
        F_count("*").alias("quake_count"),
        F_avg(col("magnitude")).alias("avg_magnitude"),
        F_max(col("magnitude")).alias("max_magnitude"),
    )
    .orderBy(col("event_day").asc())
)

logging.info("Daily statistics computed")

# ---- 3) Top places (simple normalization) ----
# place examples: "10 km SE of Town, Country"
# We can keep it as is but normalize spacing/lowercase.
places = (
    df_time.select(lower(col("place")).alias("place"))
    .filter(col("place").isNotNull() & (col("place") != ""))
    .withColumn("place", regexp_replace(col("place"), "\\s+", " "))
)

place_count = (
    places.groupBy("place")
    .agg(F_count("*").alias("count"))
    .orderBy(col("count").desc())
)

top_places = place_count.limit(20).collect()
logging.info("Top 20 places:")
for r in top_places:
    logging.info(f"Place: {r['place']}, Count: {r['count']}")

# ---- 4) Write results back to Elasticsearch ----
# Index 1: daily stats (time-series friendly)
daily_stats.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "earthquakes_daily_stats") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

logging.info("Daily stats written to Elasticsearch index: earthquakes_daily_stats")

# Index 2: place frequency
place_count.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "earthquakes_place_count") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

logging.info("Place counts written to Elasticsearch index: earthquakes_place_count")

# Stop Spark session
spark.stop()
logging.info("Spark session stopped")
logging.info("-" * 100)