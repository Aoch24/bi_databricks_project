# Databricks notebook source
# MAGIC %md
# MAGIC # Criando camadas do projeto

# COMMAND ----------

display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/bi_project")
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

dbutils.fs.mkdirs('/mnt/bi_project/bronze')
dbutils.fs.mkdirs('/mnt/bi_project/silver')
dbutils.fs.mkdirs('/mnt/bi_project/gold')
display(dbutils.fs.ls('/mnt/bi_project'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Subindo as fontes de datos a FileStore

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/datasource/')
display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/datasource/'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Encaminhando os dados para a camada bronze

# COMMAND ----------

dbutils.fs.mv('/FileStore/datasource/','/mnt/bi_project/bronze/', recurse=True)
display(dbutils.fs.ls('/mnt/bi_project/bronze'))
