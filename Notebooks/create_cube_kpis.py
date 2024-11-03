# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

df_vendas = spark.table('databricks_project.dw_atacadez.fact_venta')

df_vendas_consolidado = df_vendas.groupBy('id_loja','id_producto','id_dia').agg(f.sum('cant_vendida').alias('quantidade_vendida'),
                                                                                f.sum('valor_venta').alias('valor_venda'),
                                                                                f.sum('costo_venta').alias('custo_venda'))

df_vendas_consolidado.display()

# COMMAND ----------

df_orcado = spark.table('databricks_project.dw_atacadez.fact_orcado')

df_orcado.display()

# COMMAND ----------

df_vendas_orcado_consolidado = df_vendas_consolidado.join(df_orcado, ['id_loja','id_dia','id_producto'], 'inner').drop('id_cliente')
df_vendas_orcado_consolidado.display()

# COMMAND ----------

df_vendas_orcado_consolidado.select(f.sum('valor_venda')).display()

# COMMAND ----------

df_dim_tempo = spark.table('databricks_project.dw_atacadez.dim_tiempo')

# COMMAND ----------

from pyspark.sql import Window

# Realizar la unión entre los DataFrames
joined_df = df_vendas_orcado_consolidado.join(df_dim_tempo, "id_dia", "inner")

window_spec = Window.orderBy("cod_mes")

joined_df = joined_df.withColumn(
    "row_num", f.row_number().over(window_spec)
)

joined_df_tendencia = joined_df\
    .withColumn("quantidade_vendida_valor_anterior",
                f.when(f.col("row_num") == 1, 0)  # Asigna 0 al primer registro
                .otherwise(f.lag(f.col("quantidade_vendida"), 1).over(window_spec))
               )\
    .withColumn("valor_venda_valor_anterior",
                f.when(f.col("row_num") == 1, 0)  # Asigna 0 al primer registro
                .otherwise(f.lag(f.col("valor_venda"), 1).over(window_spec))
               ).drop("row_num")

# COMMAND ----------

resultado_agrupado = joined_df_tendencia.groupBy('cod_mes')\
    .agg(
        f.sum('valor_venda').alias('total_valor_venda'),
        f.sum('valor_venda_valor_anterior').alias('total_valor_venda_anterior')
    )
resultado_agrupado = resultado_agrupado.withColumn("tendencia", f.when(f.col("total_valor_venda") > f.col("total_valor_venda_anterior"), 1)
               .when(f.col("total_valor_venda") < f.col("total_valor_venda_anterior"), -1)
               .otherwise(0))
# Mostrar el resultado
display(resultado_agrupado)

# COMMAND ----------


cubo_kpi = joined_df_tendencia.cube("id_loja", "id_producto", "cod_mes", "cod_trimestre", "cod_semestre", "cod_ano")\
        .agg(
        f.sum("quantidade_vendida").alias("quantidade_vendida"),
        f.sum("valor_venda").alias("valor_venda"),
        f.sum("custo_venda").alias("custo_venda"),
        f.sum("quantidade_vendida_orcado").alias("quantidade_vendida_orcado"),
        f.sum("valor_venda_orcado").alias("valor_venda_orcado"),
        f.sum("custo_venda_orcado").alias("custo_venda_orcado"),
        (f.sum("quantidade_vendida") / f.sum("quantidade_vendida_orcado")).alias('kpi_quantidade_real_orcado'),
        (f.sum("valor_venda") / f.sum("valor_venda_orcado")).alias('kpi_valor_venda_real_orcado'),
        f.sum("quantidade_vendida_valor_anterior").alias("quantidade_vendida_valor_anterior"),
        f.sum("valor_venda_valor_anterior").alias("valor_venda_valor_anterior")
    )\
    .orderBy("id_loja", "id_producto", "cod_mes", "cod_trimestre", "cod_semestre", "cod_ano")


# COMMAND ----------

display(cubo_kpi)

# COMMAND ----------

cubo_kpi_status = cubo_kpi.withColumn(
    "quantidade_vendida_status",
    f.when(f.col("kpi_quantidade_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_quantidade_real_orcado") > 0.9) & (f.col("kpi_quantidade_real_orcado") < 1.1), 0)
     .when(f.col("kpi_quantidade_real_orcado") > 1, 1)
)\
.withColumn(
    "valor_venda_status",
    f.when(f.col("kpi_valor_venda_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_valor_venda_real_orcado") > 0.9) & (f.col("kpi_valor_venda_real_orcado") < 1.1), 0)
     .when(f.col("kpi_valor_venda_real_orcado") > 1, 1))

# COMMAND ----------

cubo_kpi_tendencia = cubo_kpi_status.withColumn("quantidade_vendida_tendencia", 
                        f.when(f.col("quantidade_vendida") > f.col("quantidade_vendida_valor_anterior"), 1)
                        .when(f.col("quantidade_vendida") < f.col("quantidade_vendida_valor_anterior"), -1)
                        .otherwise(0))\
                                    .withColumn("valor_venda_tendencia",
                                                f.when(f.col("valor_venda") > f.col("valor_venda_valor_anterior"), 1)
                        .when(f.col("valor_venda") < f.col("valor_venda_valor_anterior"), -1)
                        .otherwise(0))



# COMMAND ----------

display(cubo_kpi_tendencia)

# COMMAND ----------

cubo_kpi_tendencia.write.format("delta").mode("overwrite").saveAsTable("databricks_project.dw_atacadez.cubo_kpi")
