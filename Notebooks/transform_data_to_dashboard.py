# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

spark.sql("USE databricks_project.dw_atacadez")

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

df_tendencia_presente = spark.sql("""
SELECT        
fact_tendencia.id_loja, 
fact_tendencia.id_cliente, 
fact_tendencia.id_producto, 
dim_tempo_2.id_dia,
fact_tendencia.quantidade_vendida_tend, 
fact_tendencia.valor_venda_tend, 
fact_tendencia.custo_venda_tend
FROM            
fact_tendencia 
INNER JOIN dim_tiempo ON fact_tendencia.id_dia = dim_tiempo.id_dia 
INNER JOIN dim_tiempo as dim_tempo_2 ON DATEADD(MONTH, -6, dim_tiempo.dia) = dim_tempo_2.dia
                                """)

# COMMAND ----------

display(df_tendencia_presente)

# COMMAND ----------

df_tendencia_presente.createOrReplaceTempView("tendencia_presente_view")

# COMMAND ----------

df_kpis_vendas = spark.sql("""
SELECT
tp.id_loja,
tp.id_dia,
tp.id_producto,
v.cant_vendida,
v.valor_venta,
v.costo_venta,
o.quantidade_vendida_orcado,
o.valor_venda_orcado,
o.custo_venda_orcado,
tp.quantidade_vendida_tend,
tp.valor_venda_tend,
tp.custo_venda_tend
FROM tendencia_presente_view tp
INNER JOIN fact_venta v ON tp.id_loja = v.id_loja AND tp.id_dia = v.id_dia AND tp.id_producto = v.id_producto
INNER JOIN fact_orcado o ON tp.id_loja = o.id_loja AND tp.id_dia = o.id_dia AND tp.id_producto = o.id_producto
          """)

display(df_kpis_vendas)

# COMMAND ----------

df_kpis_vendas = df_kpis_vendas.withColumn('kpi_quantidade_real_orcado',f.col('cant_vendida') / f.col('quantidade_vendida_orcado'))\
                                .withColumn('kpi_valor_venda_real_orcado',f.col('valor_venta') / f.col('valor_venda_orcado'))\
                                .withColumn('kpi_quantidade_tendencia_orcado',f.col('quantidade_vendida_tend') / f.col('quantidade_vendida_orcado'))\
                                .withColumn('kpi_valor__venda_tendencia_orcado',f.col('valor_venda_tend') / f.col('valor_venda_orcado'))

display(df_kpis_vendas)

# COMMAND ----------

display(df_kpis_vendas)
