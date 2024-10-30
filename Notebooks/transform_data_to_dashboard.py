# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

df_vendas = spark.table('databricks_project.dw_atacadez.fact_venta')

df_vendas.display()

# COMMAND ----------

df_vendas = df_vendas.withColumn('preco_unitario_venda', f.col('valor_venta') / f.col('cant_vendida'))\
                        .withColumn('preco_unitario_compra', f.col('costo_venta') / f.col('cant_vendida'))\
                        .withColumn('lucro', f.col('valor_venta') - f.col('costo_venta'))\
                        .withColumn('rentabilidade', (f.col('lucro') / f.col('valor_venta')) * 100)

df_vendas.display()

# COMMAND ----------

df_vendas.display()

# COMMAND ----------


