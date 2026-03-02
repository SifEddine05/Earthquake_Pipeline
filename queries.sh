#!/bin/bash

echo "1) Requête textuelle (match sur place)"
curl -X POST "http://localhost:9200/earthquakes/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "place": "California"
      }
    }
  }'

echo -e "\n--------------------------------------------------\n"

echo "2) Requête avec agrégation (magnitude moyenne + par type)"
curl -X POST "http://localhost:9200/earthquakes/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "aggs": {
      "avg_magnitude": {
        "avg": { "field": "magnitude" }
      },
      "by_type": {
        "terms": {
          "field": "type",
          "size": 10
        }
      }
    }
  }'

echo -e "\n--------------------------------------------------\n"

echo "3) Requête N-gram (recherche partielle)"
curl -X POST "http://localhost:9200/earthquakes/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "place.ngram": "calif"
      }
    }
  }'

echo -e "\n--------------------------------------------------\n"

echo "4) Requête floue (fuzzy)"
curl -X POST "http://localhost:9200/earthquakes/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "place.fuzzy": {
          "query": "Califronia",
          "fuzziness": "AUTO"
        }
      }
    }
  }'

echo -e "\n--------------------------------------------------\n"

echo "5) Série temporelle (séismes sur 7 jours)"
curl -X POST "http://localhost:9200/earthquakes/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "size": 0,
    "query": {
      "range": {
        "@timestamp": {
          "gte": "now-7d/d",
          "lte": "now/d"
        }
      }
    },
    "aggs": {
      "quakes_per_day": {
        "date_histogram": {
          "field": "@timestamp",
          "calendar_interval": "day"
        }
      }
    }
  }'

echo -e "\n--------------------------------------------------\n"
echo "Done."