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

log_info("Iniciando proceso de ingestión de asset")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("use_mock", "true")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
use_mock = dbutils.widgets.get("use_mock", "true")
today = date.today().strftime("%Y/%m/%d")

log_info(f"Parámetros recibidos: source_id={source_id}, asset_id={asset_id}, use_mock={use_mock}")
log_info(f"Fecha de ejecución: {today}")

# Leer metadata
log_info("Cargando metadata de sources y assets")
try:
    df_sources = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.source")
    df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.asset")

    log_info(f"Obteniendo información para source: {source_id}")
    source = get_source_info(df_sources, source_id)
    if not source:
        log_error(f"No se encontró información para el source: {source_id}")
        raise Exception(f"Source no encontrado: {source_id}")

    log_info(f"Obteniendo información para asset: {asset_id}")
    asset = get_source_info(df_assets, asset_id)
    if not asset:
        log_error(f"No se encontró información para el asset: {asset_id}")
        raise Exception(f"Asset no encontrado: {asset_id}")

    log_info(f"Metadata cargada correctamente")
except Exception as e:
    log_error(f"Error al cargar metadata: {str(e)}")
    raise

connector = source["connectorstring"]
type_ = source["connectortype"]
username = source.get("username")
password = source.get("password")
query = asset["query"]
asset_name = asset["assetname"]

log_info(f"Iniciando extracción para asset: {asset_name} desde {type_}")
log_info(f"Conector: {connector}")

# Selector de función de conexión
try:
    if type_ in ["sqlserver", "postgresql", "mysql", "oracle", "synapse", "snowflake"]:
        log_info(f"Conectando a base de datos {type_}")
        df = connect_jdbc(spark, connector, query)
    elif type_ == "delta":
        log_info("Conectando a origen Delta")
        df = connect_delta(spark, query, is_catalog=True)
    elif type_ == "parquet":
        log_info("Conectando a origen Parquet")
        df = connect_parquet(spark, query)
    elif type_ == "csv":
        log_info("Conectando a origen CSV")
        df = connect_csv(spark, query)
    elif type_ == "json":
        log_info("Conectando a origen JSON")
        df = connect_json(spark, query)
    elif type_ == "rest_api":
        log_info("Conectando a API REST")
        df = connect_rest_api(spark, connector)
    elif type_ == "graphql_api":
        log_info("Conectando a API GraphQL")
        df = connect_graphql_api(spark, connector, query)
    elif type_ == "olap_cube":
        if use_mock == "true":
            log_info("Conectando a OLAP XMLA (modo mock)")
            df = connect_olap_xmla_mock(spark, connector, query, username, password)
        else:
            log_info("Conectando a OLAP XMLA")
            df = connect_olap_xmla(spark, connector, query, username, password)
    else:
        log_error(f"Tipo de conector no soportado: {type_}")
        raise Exception(f"Tipo de conector no soportado: {type_}")

    row_count = df.count()
    log_info(f"Datos extraídos correctamente. Número de registros: {row_count}")
except Exception as e:
    log_error(f"Error al extraer datos desde {type_}: {str(e)}")
    raise

# Escritura de datos
output_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={today}/"
log_info(f"Preparando escritura de datos en: {output_path}")

try:
    log_info("Añadiendo columna de fecha de ingesta")
    df = df.withColumn("ingestion_date", lit(today))

    log_info("Iniciando escritura de datos en formato Parquet")
    df.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)
    log_info(f"Datos escritos correctamente en: {output_path}")
except Exception as e:
    log_error(f"Error al escribir datos: {str(e)}")
    raise

log_info(f"Proceso de ingestión completado exitosamente para asset: {asset_name}")