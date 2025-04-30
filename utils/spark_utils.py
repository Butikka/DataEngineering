# utils/spark_utils.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_replace, sha2, concat_ws, lit, current_timestamp,
    col, when, to_date, to_timestamp, date_format, format_number, lpad
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Utility
def start_spark(app_name="MyApp"):
    return SparkSession.builder.appName(app_name).getOrCreate()
