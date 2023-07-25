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
mount_point = "/mnt/test_db_2.0/output",
extra_configs = {"fs.azure.account.key.saakshistorage2.blob.core.windows.net":"/gt6jmiEPXOIuYd+agoYPsUwOt9QrnOVFqFrK3yRIXNYdHbpVpHABuwvdKSK8JGpwutO/MDgMBPl+AStp+A5AQ=="})

# COMMAND ----------

from pyspark.sql.functions import *
import urllib

df1 = spark.read.format("csv").option("sep",",").option("header","true").option("InferSchema","true").load("/mnt/test_db_2.0/output")
display(df1)

# COMMAND ----------

df1.show(2)

# COMMAND ----------

# selecting specific columns
df1.select('name','department').show()

# COMMAND ----------

df1.collect()

# COMMAND ----------

# "First two rows"
df1.take(2)

# "Last two rows"
df1.tail(2)

# COMMAND ----------

# act as info in pandas
df1.describe().show()

# COMMAND ----------

# group by

df1.groupby("location").avg().show()

# COMMAND ----------

# filter
df1.filter(df1.user == 1).show()

# COMMAND ----------

df1.filter(df1.name == 'mani').show()

# COMMAND ----------

df1.filter(df1.salary >=10000).show()

# COMMAND ----------

# rename
df2 = df1.withColumnRenamed('location','loc')

# COMMAND ----------

df2.show()

# COMMAND ----------

# sort
df1.sort(df1.salary.desc()).show()

# COMMAND ----------

df1.orderBy(df1.salary.desc()).show()

# COMMAND ----------

# metrics -- find out total salary based on location 
df1.groupby('location').sum('salary').show()

# COMMAND ----------

df1.groupby('location','date').sum('salary').show()

# COMMAND ----------

df1.groupby('location','attendance').sum('salary').show()

# COMMAND ----------


