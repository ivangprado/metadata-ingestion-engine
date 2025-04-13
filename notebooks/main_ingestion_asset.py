# notebooks/main_ingestion_asset.py

import sys
sys.path.append("/Workspace/Repos/<tu_usuario>/metadata-ingestion-engine")

from connectors.olap import connect_olap_xmla, connect_olap_xmla_mock
from config.settings import JDBC_URL, JDBC_DRIVER, RAW_BASE_PATH
from metadata.reader import load_metadata, get_source_info
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date

spark = SparkSession.builder.getOrCreate()

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
use_mock = dbutils.widgets.get("use_mock", "true")
today = date.today().strftime("%Y/%m/%d")

# Leer metadata
df_sources = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "dbo.source")
df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "dbo.asset")
source = get_source_info(df_sources, source_id)
asset = get_source_info(df_assets, asset_id)

connector = source["connectorstring"]
type_ = source["connectortype"]
username = source.get("username")
password = source.get("password")
query = asset["query"]
asset_name = asset["assetname"]

if type_ == "olap_cube":
    df = connect_olap_xmla_mock(spark, connector, query, username, password) if use_mock == "true" \
         else connect_olap_xmla(spark, connector, query, username, password)
else:
    raise Exception(f"Tipo de conector no implementado en demo: {type_}")

output_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={today}/"
df = df.withColumn("ingestion_date", lit(today))
df.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)

print(f"[OK] Datos de {asset_name} escritos en: {output_path}")
