# Databricks notebook source
# MAGIC %md
# MAGIC #Getting Started
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema madhavi

# COMMAND ----------

data=([(1,'a',30),(2,'b',34)])
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)

# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/raw/

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/processed")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/processed/","dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/raw/emp.csv")

# COMMAND ----------

df.write.saveAsTable("madhavi.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from madhavi.emp_demo

# COMMAND ----------


