# COMMAND ----------

# notebooks/main_ingestion_dispatcher.py

import concurrent.futures
import sys
import getpass

user = getpass.getuser()
sys.path.append("/Workspace/Repos/<tu_usuario>/metadata-ingestion-engine")

from metadata.reader import load_metadata, get_asset_list
from utils.logger import log_info, log_warning, log_error
from config.settings import JDBC_URL, JDBC_DRIVER
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

log_info("Starting ingestion dispatching process")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("max_threads", "4")
dbutils.widgets.text("use_mock", "true")

source_id = dbutils.widgets.get("sourceid")
max_threads_str = dbutils.widgets.get("max_threads")
max_threads = int(max_threads_str) if max_threads_str.strip() else 4
use_mock = dbutils.widgets.get("use_mock")

log_info(f"Parameters received: source_id={source_id}, max_threads={max_threads}, use_mock={use_mock}")

# COMMAND ----------

try:
    log_info("Loading assets metadata")
    df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.asset")
    assets = get_asset_list(df_assets, source_id)

    num_assets = len(assets)
    log_info(f"Found {num_assets} assets to process from source {source_id}")

    if num_assets == 0:
        log_warning(f"No assets found for source {source_id}")


    def run_asset(asset):
        asset_id = asset["assetid"]
        asset_name = asset.get("assetname", "unknown")
        log_info(f"Launching process for asset: {asset_name} (ID: {asset_id})")
        try:
            result = dbutils.notebook.run("../notebooks/main_ingestion_asset", 3600, {
                "sourceid": source_id,
                "assetid": asset_id,
                "use_mock": use_mock
            })
            log_info(f"Processing completed for asset {asset_id}: {result}")
            return result
        except Exception as e:
            log_error(f"Error processing asset {asset_id}: {str(e)}")
            return f"ERROR: {str(e)}"


    log_info(f"Starting parallel processing with {max_threads} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_asset, row.asDict()) for row in assets]

        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            log_info(f"Progress: {completed}/{num_assets} assets completed")

        concurrent.futures.wait(futures)

    log_info(f"Dispatching process completed. Total processed: {num_assets} assets")

except Exception as e:
    log_error(f"Critical error in dispatching process: {str(e)}")
    raise