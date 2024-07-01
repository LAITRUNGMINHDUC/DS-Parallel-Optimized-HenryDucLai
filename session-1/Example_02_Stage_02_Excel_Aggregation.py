# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# Decorator to calculate running time.
def with_time_review(func):
    import time
    import traceback

    def wrapper(*args, **kwargs):
        begin = time.time()
        result = func(*args, **kwargs)
        end = round(time.time() - begin, 5)

        print(f"Function: '{func.__name__}' runs for: {end} seconds.")
        print("----------------------------\n")
        return result

    return wrapper

# COMMAND ----------

import ray
import pyspark.pandas as ps_pd
import pandas as pd

ray.shutdown()
ray.init(
    log_to_driver=False,
    ignore_reinit_error=True,
    runtime_env={
        "pip": ["openpyxl"],
        "env_vars": {
            "PYTHONHASHSEED": "0",
        },
    },
)

# COMMAND ----------

import glob
import os

INPUT_FOLDER = "mnt/adls_gen2/HENRYDUCLAI/TRAINING/SESSION_01/EXCEL_DATA"
OUTPUT_FOLDER = "mnt/adls_gen2/HENRYDUCLAI/TRAINING/SESSION_01/PARQUET_DATA"
os.makedirs("/dbfs/" + OUTPUT_FOLDER, exist_ok=True)

list_excel_files = glob.glob("/dbfs/" + INPUT_FOLDER + "/*")
spark_df_filepath = spark.createDataFrame(pd.DataFrame({"FILE_PATH": list_excel_files}))
list_excel_files

# COMMAND ----------

# This script is super slow, I don't know when it gonna finished actually.

# spark_full_excel = ps_pd.read_excel("dbfs:/" + INPUT_FOLDER)
# output_spark_excel = spark_full_excel.describe().to_pandas()
# print(output_spark_excel)

# COMMAND ----------

def convert_to_parquet(file_path):
    import time

    begin = time.time()

    df = pd.read_excel(file_path)
    file_name = file_path.split("/")[-1]
    df.to_parquet(f"/dbfs/{OUTPUT_FOLDER}/{file_name}_converted.parquet", index=False)

    output_obj = {"FILENAME": file_name, "TIME": time.time() - begin}

    return pd.DataFrame([output_obj])


convert_to_parquet(
    file_path="/dbfs/mnt/adls_gen2/HENRYDUCLAI/TRAINING/SESSION_01/EXCEL_DATA/1719834580227032945_596666.xlsx"
)

# COMMAND ----------

@with_time_review
def test_RAY(list_excel_files):
    RAY_func = ray.remote(convert_to_parquet)
    tasks = [RAY_func.remote(file_path) for file_path in list_excel_files]
    tasks = ray.get(tasks)
    return pd.concat(tasks)


@with_time_review
def test_SPARK(spark_df):
    run_spark = spark_df.groupBy("FILE_PATH").applyInPandas(
        lambda df_group: convert_to_parquet(file_path=df_group["FILE_PATH"].iloc[0]),
        schema="FILENAME string, TIME float",
    )
    run_spark = run_spark.toPandas()
    return run_spark

# COMMAND ----------

output_RAY = test_RAY(list_excel_files)

# COMMAND ----------

# Uncomment this line to test SPARK
# output_SPARK = test_SPARK(spark_df_filepath)

# COMMAND ----------

spark_full_parquet = ps_pd.read_parquet("dbfs:/" + OUTPUT_FOLDER)
output_spark_parquet = spark_full_parquet.describe().to_pandas()
display(spark_full_parquet)
print(output_spark_parquet)

# COMMAND ----------

# output_spark_parquet
