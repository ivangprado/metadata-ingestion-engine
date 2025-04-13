# connectors/file.py

def connect_parquet(spark, path: str):
    return spark.read.parquet(path)

def connect_csv(spark, path: str, header=True, inferSchema=True):
    return spark.read.option("header", header).option("inferSchema", inferSchema).csv(path)

def connect_json(spark, path: str):
    return spark.read.json(path)
