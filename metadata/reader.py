# metadata/reader.py

def load_metadata(spark, jdbc_url, driver, table_name):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("driver", driver) \
        .load()

def get_source_info(df_sources, sourceid):
    return df_sources.filter(f"sourceid = '{sourceid}'").collect()[0]

def get_asset_list(df_assets, sourceid):
    return df_assets.filter(f"sourceid = '{sourceid}'").collect()
