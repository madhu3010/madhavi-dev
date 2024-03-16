# Databricks notebook source
df = spark.read.json("dbfs:/FileStore/tables/raw/iot1.json")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.saveAsTable("madhavi.iot_demo")

# COMMAND ----------

# MAGIC %run "/Workspace/Users/madhavi.singh@mmc.com/Day1/Includes"

# COMMAND ----------

df = spark.read.json(f"{input_path}iot1.json")

# COMMAND ----------


