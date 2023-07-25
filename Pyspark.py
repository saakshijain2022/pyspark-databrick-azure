# Databricks notebook source
# data frame creation 2 nd method
from datetime import datetime, date
df= spark.createDataFrame([
    (1,2.,'string1',date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2,3.,'string2',date(2000,2,1), datetime(2000,1,2,12,0)),
    (3,4.,'string3',date(2000,3,1), datetime(2000,1,3,12,0))
    ],schema='a long, b double, c string , d date , e timestamp')

display(df)

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://raw@saakshistorage2.blob.core.windows.net",
mount_point = "/mnt/test_db_2.0/outputs",

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

df1 = spark.read.format("csv").option("sep",",").option("header","true").option("InferSchema","true").load("/mnt/test_db_2.0/outputs")
display(df1)

# COMMAND ----------

df1.show(2)

# COMMAND ----------


