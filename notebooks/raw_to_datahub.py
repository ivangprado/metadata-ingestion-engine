# COMMAND ----------

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

log_info("Starting migration process from RAW to DataHub")

dbutils.widgets.text("sourceid", "")
dbutils.widgets.text("assetid", "")
dbutils.widgets.text("assetname", "")
dbutils.widgets.text("execution_date", "")

source_id = dbutils.widgets.get("sourceid")
asset_id = dbutils.widgets.get("assetid")
asset_name = dbutils.widgets.get("assetname")
execution_date = dbutils.widgets.get("execution_date", date.today().strftime("%Y/%m/%d"))

log_info(f"Processing asset: {asset_name} (ID: {asset_id}) from source: {source_id}")
log_info(f"Execution date: {execution_date}")

# COMMAND ----------

# Read column metadata
log_info("Loading asset column metadata")
df_columns = load_metadata(spark, JDBC_URL, JDBC_DRIVER, "metadata.assetcolumns")
schema = df_columns.filter(f"assetid = '{asset_id}'").select("columnname", "ispk").collect()
pk_cols = [row["columnname"] for row in schema if row["ispk"]]

log_info(f"PK columns identified: {pk_cols}")

# COMMAND ----------

# Paths
parquet_path = f"{RAW_BASE_PATH}/{source_id}/{asset_name}/ingestion_date={execution_date}/"
delta_path = f"{SILVER_BASE_PATH}/{source_id}/{asset_name}/"

log_info(f"Source path (RAW): {parquet_path}")
log_info(f"Target path (DataHub): {delta_path}")

# COMMAND ----------

# Read Parquet
log_info("Reading data from RAW layer (Parquet)")
try:
    df = spark.read.parquet(parquet_path)
    log_info(f"Data read successfully. Number of records: {df.count()}")
except Exception as e:
    log_error(f"Error reading parquet: {str(e)}")
    raise

# COMMAND ----------

# Column validation
missing = [col["columnname"] for col in schema if col["columnname"] not in df.columns]
if missing:
    log_error(f"Missing columns in dataset: {missing}")
    raise Exception(f"[ERROR] Missing columns: {missing}")
else:
    log_info("Column validation completed successfully")

# COMMAND ----------

# SCD2
if pk_cols:
    log_info("Applying SCD Type 2 treatment")
    df = df.dropDuplicates(pk_cols)
    log_info(f"Duplicates removed by PK. Remaining records: {df.count()}")

    df = prepare_scd2_columns(df, execution_date)
    log_info("SCD2 columns added to dataframe")

    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, delta_path):
        log_info("Delta table already exists. Executing SCD2 merge")
        apply_scd2_merge(spark, df, delta_path, pk_cols)
        log_info("SCD2 merge completed successfully")
    else:
        log_info("Delta table doesn't exist. Creating new table")
        df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)
        log_info("Delta table created successfully")
else:
    log_warning("No PK columns found. Performing full overwrite")
    df.write.format("delta").partitionBy("execution_date").mode("overwrite").save(delta_path)
    log_info("Data written successfully in Delta format")

# COMMAND ----------

# Optimization
log_info("Starting Delta table optimization")
try:
    spark.sql(f"OPTIMIZE delta.`{delta_path}`")
    log_info("Optimization completed")
    spark.sql(f"VACUUM delta.`{delta_path}` RETAIN 168 HOURS")
    log_info("Vacuum completed (retention: 168 hours)")
except Exception as e:
    log_warning(f"Error during optimization: {str(e)}")

log_info(f"Process completed. Data updated in DataHub layer: {delta_path}")