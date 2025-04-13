# notebooks/raw_to_datahub.py

import sys
import getpass
user = getpass.getuser()
sys.path.append("/Workspace/Repos/<tu_usuario>/metadata-ingestion-engine")

from metadata.reader import load_metadata
from utils.scd import prepare_scd2_columns, apply_scd2_merge
from config.settings import JDBC_URL, JDBC_DRIVER, RAW_BASE_PATH, SILVER_BASE_PATH
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date

spark = SparkSession.builder.getOrCreate()

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("assetname", "")
dbutils.widgets.text("execution_date", "")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
asset_name = dbutils.widgets.get("assetname")
execution_date = dbutils.widgets.get("execution_date", date.today().strftime("%Y/%m/%d"))

# Leer metadata de columnas
df_columns = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "dbo.assetcolumns")
schema = df_columns.filter(f"assetid = '{asset_id}'").select("columnname", "ispk").collect()
pk_cols = [row["columnname"] for row in schema if row["ispk"]]

# Rutas
parquet_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={execution_date}/"
delta_path = f"{SILVER_BASE_PATH}/{source_id}/{asset_name}/"

# Leer Parquet
df = spark.read.parquet(parquet_path)

# Validación de columnas
missing = [col["columnname"] for col in schema if col["columnname"] not in df.columns]
if missing:
    raise Exception(f"[ERROR] Columnas faltantes: {missing}")

# SCD2
if pk_cols:
    df = df.dropDuplicates(pk_cols)
    df = prepare_scd2_columns(df, execution_date)

    from delta.tables import DeltaTable
    if DeltaTable.isDeltaTable(spark, delta_path):
        apply_scd2_merge(spark, df, delta_path, pk_cols)
    else:
        df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)
else:
    df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)

# Optimización
spark.sql(f"OPTIMIZE delta.`{delta_path}`")
spark.sql(f"VACUUM delta.`{delta_path}` RETAIN 168 HOURS")

print(f"[OK] Datos actualizados en capa DataHub: {delta_path}")
