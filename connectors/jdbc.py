# connectors/jdbc.py

def connect_jdbc(spark, connectorstring: str, query: str, driver: str = None):
    reader = spark.read.format("jdbc") \
        .option("url", connectorstring) \
        .option("query", query)
    if driver:
        reader = reader.option("driver", driver)
    return reader.load()