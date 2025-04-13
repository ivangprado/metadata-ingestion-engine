# COMMAND ----------

# notebooks/main_ingestion_asset.py

import sys
import getpass

user = getpass.getuser()
sys.path.append("/Workspace/Repos/ivangprado/metadata-ingestion-engine")

from connectors import (
    connect_jdbc, connect_delta, connect_parquet,
    connect_csv, connect_json, connect_rest_api,
    connect_graphql_api, connect_olap_xmla, connect_olap_xmla_mock
)
from config.settings import JDBC_URL, JDBC_DRIVER, RAW_BASE_PATH
from utils.logger import log_info, log_warning, log_error
from metadata.reader import load_metadata, get_source_info
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date

spark = SparkSession.builder.getOrCreate()

log_info("Starting asset ingestion process")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("use_mock", "true")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
use_mock = dbutils.widgets.get("use_mock", "true")
today = date.today().strftime("%Y/%m/%d")

log_info(f"Parameters received: source_id={source_id}, asset_id={asset_id}, use_mock={use_mock}")
log_info(f"Execution date: {today}")

# COMMAND ----------

# Read metadata
log_info("Loading sources and assets metadata")
try:
    df_sources = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.source")
    df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.asset")

    log_info(f"Getting information for source: {source_id}")
    source = get_source_info(df_sources, source_id)
    if not source:
        log_error(f"Information for source {source_id} not found")
        raise Exception(f"Source not found: {source_id}")

    log_info(f"Getting information for asset: {asset_id}")
    asset = get_source_info(df_assets, asset_id)
    if not asset:
        log_error(f"Information for asset {asset_id} not found")
        raise Exception(f"Asset not found: {asset_id}")

    log_info(f"Metadata loaded successfully")
except Exception as e:
    log_error(f"Error loading metadata: {str(e)}")
    raise

connector = source["connectorstring"]
type_ = source["connectortype"]
username = source.get("username")
password = source.get("password")
query = asset["query"]
asset_name = asset["assetname"]

log_info(f"Starting extraction for asset: {asset_name} from {type_}")
log_info(f"Connector: {connector}")

# COMMAND ----------

# Connector function selector
try:
    if type_ in ["sqlserver", "postgresql", "mysql", "oracle", "synapse", "snowflake"]:
        log_info(f"Connecting to {type_} database")
        df = connect_jdbc(spark, connector, query)
    elif type_ == "delta":
        log_info("Connecting to Delta source")
        df = connect_delta(spark, query, is_catalog=True)
    elif type_ == "parquet":
        log_info("Connecting to Parquet source")
        df = connect_parquet(spark, query)
    elif type_ == "csv":
        log_info("Connecting to CSV source")
        df = connect_csv(spark, query)
    elif type_ == "json":
        log_info("Connecting to JSON source")
        df = connect_json(spark, query)
    elif type_ == "rest_api":
        log_info("Connecting to REST API")
        df = connect_rest_api(spark, connector)
    elif type_ == "graphql_api":
        log_info("Connecting to GraphQL API")
        df = connect_graphql_api(spark, connector, query)
    elif type_ == "olap_cube":
        if use_mock == "true":
            log_info("Connecting to OLAP XMLA (mock mode)")
            df = connect_olap_xmla_mock(spark, connector, query, username, password)
        else:
            log_info("Connecting to OLAP XMLA")
            df = connect_olap_xmla(spark, connector, query, username, password)
    else:
        log_error(f"Unsupported connector type: {type_}")
        raise Exception(f"Unsupported connector type: {type_}")

    row_count = df.count()
    log_info(f"Data extracted successfully. Number of records: {row_count}")
except Exception as e:
    log_error(f"Error extracting data from {type_}: {str(e)}")
    raise

# COMMAND ----------

# Data writing
output_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={today}/"
log_info(f"Preparing data writing to: {output_path}")

try:
    log_info("Adding ingestion date column")
    df = df.withColumn("ingestion_date", lit(today))

    log_info("Starting data writing in Parquet format")
    df.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)
    log_info(f"Data written successfully to: {output_path}")
except Exception as e:
    log_error(f"Error writing data: {str(e)}")
    raise

log_info(f"Ingestion process completed successfully for asset: {asset_name}")
print(f"[OK] {asset_name} data written to: {output_path}")