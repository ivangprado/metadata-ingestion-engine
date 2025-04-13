# notebooks/raw_to_datahub.py

import sys
import getpass

user = getpass.getuser()
sys.path.append("/Workspace/Repos/<tu_usuario>/metadata-ingestion-engine")

from metadata.reader import load_metadata
from utils.logger import log_info, log_warning, log_error
from utils.scd import prepare_scd2_columns, apply_scd2_merge
from config.settings import JDBC_URL, JDBC_DRIVER, RAW_BASE_PATH, SILVER_BASE_PATH
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date

spark = SparkSession.builder.getOrCreate()

log_info("Iniciando proceso de migración de RAW a DataHub")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("assetname", "")
dbutils.widgets.text("execution_date", "")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
asset_name = dbutils.widgets.get("assetname")
execution_date = dbutils.widgets.get("execution_date", date.today().strftime("%Y/%m/%d"))

log_info(f"Procesando asset: {asset_name} (ID: {asset_id}) de source: {source_id}")
log_info(f"Fecha de ejecución: {execution_date}")

# Leer metadata de columnas
log_info("Cargando metadata de columnas del asset")
df_columns = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.assetcolumns")
schema = df_columns.filter(f"assetid = '{asset_id}'").select("columnname", "ispk").collect()
pk_cols = [row["columnname"] for row in schema if row["ispk"]]

log_info(f"Columnas PK identificadas: {pk_cols}")

# Rutas
parquet_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={execution_date}/"
delta_path = f"{SILVER_BASE_PATH}/{source_id}/{asset_name}/"

log_info(f"Ruta origen (RAW): {parquet_path}")
log_info(f"Ruta destino (DataHub): {delta_path}")

# Leer Parquet
log_info("Leyendo datos desde capa RAW (Parquet)")
try:
    df = spark.read.parquet(parquet_path)
    log_info(f"Datos leídos correctamente. Número de registros: {df.count()}")
except Exception as e:
    log_error(f"Error al leer parquet: {str(e)}")
    raise

# Validación de columnas
missing = [col["columnname"] for col in schema if col["columnname"] not in df.columns]
if missing:
    log_error(f"Columnas faltantes en el dataset: {missing}")
    raise Exception(f"[ERROR] Columnas faltantes: {missing}")
else:
    log_info("Validación de columnas completada correctamente")

# SCD2
if pk_cols:
    log_info("Aplicando tratamiento SCD Tipo 2")
    df = df.dropDuplicates(pk_cols)
    log_info(f"Eliminados duplicados por PK. Registros restantes: {df.count()}")

    df = prepare_scd2_columns(df, execution_date)
    log_info("Columnas SCD2 añadidas al dataframe")

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, delta_path):
        log_info("Tabla Delta ya existe. Ejecutando merge SCD2")
        apply_scd2_merge(spark, df, delta_path, pk_cols)
        log_info("Merge SCD2 completado exitosamente")
    else:
        log_info("Tabla Delta no existe. Creando nueva tabla")
        df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)
        log_info("Tabla Delta creada correctamente")
else:
    log_warning("No se encontraron columnas PK. Realizando sobrescritura completa")
    df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)
    log_info("Datos escritos correctamente en formato Delta")

# Optimización
log_info("Iniciando optimización de tabla Delta")
try:
    spark.sql(f"OPTIMIZE delta.`{delta_path}`")
    log_info("Optimización completada")
    spark.sql(f"VACUUM delta.`{delta_path}` RETAIN 168 HOURS")
    log_info("Vacuum completado (retención: 168 horas)")
except Exception as e:
    log_warning(f"Error durante la optimización: {str(e)}")

log_info(f"Proceso completado. Datos actualizados en capa DataHub: {delta_path}")
log_info(f"[OK] Datos actualizados en capa DataHub: {delta_path}")