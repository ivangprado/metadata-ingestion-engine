# utils/scd.py

from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

def prepare_scd2_columns(df, execution_date):
    now = current_timestamp()
    return df.withColumn("execution_date", lit(execution_date)) \
             .withColumn("is_current", lit(True)) \
             .withColumn("start_date", now) \
             .withColumn("end_date", lit(None).cast("timestamp"))

def apply_scd2_merge(spark, df, delta_path, pk_columns):
    now = current_timestamp()
    delta_table = DeltaTable.forPath(spark, delta_path)

    merge_condition = " AND ".join([f"target.{col} = updates.{col}" for col in pk_columns]) + " AND target.is_current = true"

    delta_table.alias("target").merge(
        source=df.alias("updates"),
        condition=merge_condition
    ).whenMatchedUpdate(set={
        "is_current": lit(False),
        "end_date": now
    }).whenNotMatchedInsertAll().execute()
