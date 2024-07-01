# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import ray
import os
import shutil
import pandas as pd 
import numpy as np
import time

ray.shutdown()
ray.init(
    log_to_driver=False,
    ignore_reinit_error=True,
    runtime_env={
        "pip": ["openpyxl"],        
        },
)

# COMMAND ----------

OUTPUT_FOLDER = 'mnt/adls_gen2/HENRYDUCLAI/TRAINING/SESSION_01/EXCEL_DATA'
os.makedirs('/dbfs/' + OUTPUT_FOLDER, exist_ok=True)

# COMMAND ----------

@ray.remote
def build_excel_file():
    sheet_name = 'DATA'
    size_df = np.random.randint(100000, 700000)
    file_name = f'{time.time_ns()}_{size_df}' 
    dbfs_directory = OUTPUT_FOLDER

    df = pd.DataFrame(np.random.randint(0,1000000,size=(size_df, 4)), columns=list('ABCD'))
    df.to_excel(f"/tmp/{file_name}.xlsx", index=False, sheet_name=sheet_name)
    shutil.copy(f"/tmp/{file_name}.xlsx", '/dbfs/' + dbfs_directory + f"/{file_name}.xlsx")

    return 1

# COMMAND ----------

tasks = [build_excel_file.remote() for _ in range(100)]
tasks = ray.get(tasks)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/" + OUTPUT_FOLDER))
