# 🌍 Real-Time Seismic Analytics

## Distributed Big Data Pipeline for Earthquake Monitoring

This project implements a real-time Big Data pipeline that collects
earthquake data from the USGS public API, streams it through Apache
Kafka, processes it with Logstash, stores it in Elasticsearch,
visualizes it using Kibana, and performs distributed analytics using
Apache Spark.

------------------------------------------------------------------------

# 🏗 Architecture Overview

USGS API\
↓\
FastAPI (Earthquake API)\
↓\
Kafka (Streaming Layer)\
↓\
Logstash (Processing Layer)\
↓\
Elasticsearch (Storage & Indexing)\
↓\
Kibana (Visualization)\
↓\
Spark (Distributed Analytics)

------------------------------------------------------------------------

# 🎯 Objectives

-   Collect earthquake data from a public API
-   Stream events in real time using Kafka
-   Process and transform data using Logstash
-   Index data into Elasticsearch
-   Visualize trends in Kibana
-   Perform distributed analytics with Spark
-   Demonstrate scalable event-driven architecture

------------------------------------------------------------------------

# ⚙️ Technologies Used

-   FastAPI
-   USGS Earthquake API
-   Apache Kafka (KRaft mode)
-   Logstash
-   Elasticsearch
-   Kibana
-   Apache Spark
-   Docker & Docker Compose

------------------------------------------------------------------------

# 📥 Data Source

USGS Earthquake API:

https://earthquake.usgs.gov/fdsnws/event/1/query

Retrieved data includes: - Event ID - Location - Magnitude - Timestamp -
Coordinates - Event type - Tsunami flag

------------------------------------------------------------------------

# 🚀 Setup & Execution

## 1️⃣ Configure .env

Create a `.env` file:

    KAFKA_BROKERS=kafka:9092
    EARTHQUAKE_API=http://earthquake-api:8000
    ES_ENDPOINT=http://elasticsearch:9200
    REQUEST_NEW_DATA_HOURS=02:00
    MIN_MAGNITUDE=0.0

## 2️⃣ Start Services

    docker compose up -d --build

Check running containers:

    docker compose ps

------------------------------------------------------------------------

# 🔄 Trigger Data Collection

Manual trigger:

    curl -X POST "http://localhost:8000/send_data?start_date=01/03/2026&end_date=02/03/2026&min_magnitude=2.5"

Kafka topic used:

    earthquakes

------------------------------------------------------------------------

# 🔍 Elasticsearch Verification

List indices:

    curl "http://localhost:9200/_cat/indices?v"

Count documents:

    curl "http://localhost:9200/earthquakes/_count?pretty"

------------------------------------------------------------------------

# 📊 Kibana

Access:

http://localhost:5601

Create Data View: - Index: earthquakes - Time field: @timestamp

Example dashboards: - Earthquakes per day - Average magnitude over
time - Top affected locations

------------------------------------------------------------------------

# ⚡ Spark Analytics

Run Spark job:

    docker exec -it spark-master spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.10.2 /opt/spark/job.py

Spark UI: - http://localhost:8080 - http://localhost:8081

------------------------------------------------------------------------

# 📦 Project Structure

    earthquake_api/
    logstash/
    schedulerService/
    spark_jobs/
    docker-compose.yml
    .env
    README.md

------------------------------------------------------------------------

# 🧠 Big Data Concepts

-   Event-driven architecture
-   Real-time streaming
-   Distributed messaging
-   Search & indexing
-   Time-series analytics
-   Distributed computation
-   Containerized microservices

------------------------------------------------------------------------

# 🏁 Conclusion

This project demonstrates a complete real-time distributed Big Data
pipeline for seismic monitoring using modern data engineering
technologies.
