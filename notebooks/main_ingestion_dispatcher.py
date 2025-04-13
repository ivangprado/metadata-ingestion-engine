# notebooks/main_ingestion_dispatcher.py

import concurrent.futures
import sys
sys.path.append("/Workspace/Repos/<tu_usuario>/metadata-ingestion-engine")

from metadata.reader import load_metadata, get_asset_list
from config.settings import JDBC_URL, JDBC_DRIVER
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

source_id = dbutils.widgets.get("sourceid")
max_threads = int(dbutils.widgets.get("max_threads", "4"))
use_mock = dbutils.widgets.get("use_mock", "true")

df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "dbo.asset")
assets = get_asset_list(df_assets, source_id)

def run_asset(asset):
    asset_id = asset["assetid"]
    return dbutils.notebook.run("../notebooks/main_ingestion_asset", 3600, {
        "sourceid": source_id,
        "assetid": asset_id,
        "use_mock": use_mock
    })

with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
    futures = [executor.submit(run_asset, row.asDict()) for row in assets]
    concurrent.futures.wait(futures)
