# connectors/api.py
import requests
import json

def connect_rest_api(spark, url: str):
    response = requests.get(url)
    data = response.json()
    return spark.read.json(spark.sparkContext.parallelize([data]))

def connect_graphql_api(spark, url: str, query: str):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({"query": query})
    response = requests.post(url, headers=headers, data=payload)
    data = response.json()
    return spark.read.json(spark.sparkContext.parallelize([data]))
