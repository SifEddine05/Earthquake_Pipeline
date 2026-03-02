import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, date_format, coalesce, lit,
    count as F_count, avg as F_avg, max as F_max, try_to_timestamp
)

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


spark = (
    SparkSession.builder.appName("EarthquakesSparkJob")
    .config("spark.es.nodes", "elasticsearch")
    .config("spark.es.port", "9200")
    .config("spark.es.nodes.wan.only", "true")
    .getOrCreate()
)

logging.info("-" * 100)
logging.info("STARTING EARTHQUAKES SPARK JOB")
logging.info("-" * 100)

# Read from Elasticsearch
df = (
    spark.read.format("org.elasticsearch.spark.sql")
    .option("es.resource", "earthquakes")
    .load()
)

total = df.count()
logging.info(f"Data read from Elasticsearch: {total} rows")

if total == 0:
    logging.warning("No data found in index 'earthquakes'. Exiting.")
    spark.stop()
    raise SystemExit(0)

# ---- 1) Parse time_iso safely ----
# Normalize:
# - remove trailing Z
# - replace T with space
ts_norm = regexp_replace(
    regexp_replace(col("time_iso").cast("string"), "Z$", ""),
    "T", " "
)

event_ts = coalesce(
    try_to_timestamp(ts_norm, lit("yyyy-MM-dd HH:mm:ss.SSS")),
    try_to_timestamp(ts_norm, lit("yyyy-MM-dd HH:mm:ss")),
    try_to_timestamp(ts_norm)  # fallback (Spark inference)
)

df_time = df.withColumn("event_ts", event_ts).filter(col("event_ts").isNotNull())

valid = df_time.count()
logging.info(f"Rows with valid parsed timestamp: {valid}")

if valid == 0:
    logging.error("All rows failed timestamp parsing. Check 'time_iso' values.")
    spark.stop()
    raise SystemExit(1)

# Day column
df_time = df_time.withColumn("event_day", date_format(col("event_ts"), "yyyy-MM-dd"))

# ---- 2) Daily stats ----
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

# ---- 3) Top places ----
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
daily_stats.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "earthquakes_daily_stats") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

logging.info("Daily stats written to Elasticsearch index: earthquakes_daily_stats")

place_count.write.format("org.elasticsearch.spark.sql") \
    .option("es.resource", "earthquakes_place_count") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .mode("overwrite") \
    .save()

logging.info("Place counts written to Elasticsearch index: earthquakes_place_count")

spark.stop()
logging.info("Spark session stopped")
logging.info("-" * 100)