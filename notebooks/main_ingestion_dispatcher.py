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

log_info("Iniciando proceso de dispatching de ingestión")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("max_threads", "4")
dbutils.widgets.text("use_mock", "true")

source_id = dbutils.widgets.get("sourceid")
max_threads_str = dbutils.widgets.get("max_threads")
max_threads = int(max_threads_str) if max_threads_str.strip() else 4
use_mock = dbutils.widgets.get("use_mock", "true")

log_info(f"Parámetros recibidos: source_id={source_id}, max_threads={max_threads}, use_mock={use_mock}")

try:
    log_info("Cargando metadata de assets")
    df_assets = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.asset")
    assets = get_asset_list(df_assets, source_id)

    num_assets = len(assets)
    log_info(f"Se encontraron {num_assets} assets para procesar en la fuente {source_id}")

    if num_assets == 0:
        log_warning(f"No se encontraron assets para la fuente {source_id}")


    def run_asset(asset):
        asset_id = asset["assetid"]
        asset_name = asset.get("assetname", "desconocido")
        log_info(f"Lanzando procesamiento para asset: {asset_name} (ID: {asset_id})")
        try:
            result = dbutils.notebook.run("../notebooks/main_ingestion_asset", 3600, {
                "sourceid": source_id,
                "assetid": asset_id,
                "use_mock": use_mock
            })
            log_info(f"Procesamiento completado para asset {asset_id}: {result}")
            return result
        except Exception as e:
            log_error(f"Error al procesar asset {asset_id}: {str(e)}")
            return f"ERROR: {str(e)}"


    log_info(f"Iniciando procesamiento en paralelo con {max_threads} hilos")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(run_asset, row.asDict()) for row in assets]

        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            log_info(f"Progreso: {completed}/{num_assets} assets completados")

        concurrent.futures.wait(futures)

    log_info(f"Proceso de dispatching completado. Total procesados: {num_assets} assets")

except Exception as e:
    log_error(f"Error crítico en el proceso de dispatching: {str(e)}")
    raise