# connectors/delta.py

def connect_delta(spark, table_path_or_name: str, is_catalog: bool = True):
    if is_catalog:
        return spark.read.format("delta").table(table_path_or_name)
    else:
        return spark.read.format("delta").load(table_path_or_name)
