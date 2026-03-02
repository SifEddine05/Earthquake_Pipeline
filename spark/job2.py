from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, avg, length
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Spark session
spark = (
    SparkSession.builder.appName("EarthquakesSparkJob")
    .config("spark.es.nodes", "elasticsearch")
    .config("spark.es.port", "9200")
    .config("spark.es.nodes.wan.only", "true")
    .getOrCreate()
)

logging.info("-" * 100)
logging.info("STARTING EARTHQUAKES JOB")
logging.info("-" * 100)
logging.info("Spark session initialized")

# Read data from Elasticsearch (index created by Logstash)
df = (
    spark.read.format("org.elasticsearch.spark.sql")
    .option("es.resource", "earthquakes")
    .load()
)
logging.info("Data read from Elasticsearch")

# ----- Example transformation -----
# Filter earthquakes by region keyword in 'place' (case-insensitive)
REGION = "california"   # change to what you want (ex: "algeria", "japan", "turkey")
filtered_df = df.filter(lower(col("place")).contains(REGION))

# Compute average magnitude in that region
avg_mag = filtered_df.select(avg(col("magnitude")).alias("avg_magnitude")).collect()[0]["avg_magnitude"]

# Optionally: average title length (keeps the "text length" idea)
avg_title_len = filtered_df.select(avg(length(col("title"))).alias("avg_title_length")).collect()[0]["avg_title_length"]

logging.info(f"Region filter: '{REGION}'")
logging.info(f"Average magnitude in '{REGION}': {avg_mag}")
logging.info(f"Average title length in '{REGION}': {avg_title_len}")

# Stop Spark session
spark.stop()
logging.info("Spark session stopped")
logging.info("-" * 100)