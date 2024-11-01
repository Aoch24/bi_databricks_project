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

df_vendas_realizadas = spark.sql("""
SELECT
id_loja,
id_dia,
id_producto,
sum(cant_vendida) as cant_vendida,
sum(valor_venta) as valor_venta,
sum(costo_venta) as costo_venta
FROM
    fact_venta
GROUP BY id_loja, id_dia, id_producto
ORDER BY id_dia, id_loja, id_producto
    """)


# COMMAND ----------

df_vendas_realizadas.createOrReplaceTempView("vendas_realizadas_view")

# COMMAND ----------

df_kpis_vendas = spark.sql("""
SELECT
    COALESCE(v.id_loja, o.id_loja) AS id_loja,
    COALESCE(v.id_dia, o.id_dia) AS id_dia,
    COALESCE(v.id_producto, o.id_producto) AS id_producto,
    v.cant_vendida,
    v.valor_venta,
    v.costo_venta,
    o.quantidade_vendida_orcado,
    o.valor_venda_orcado,
    o.custo_venda_orcado,
    tp.quantidade_vendida_tend,
    tp.valor_venda_tend,
    tp.custo_venda_tend
FROM
    vendas_realizadas_view v
FULL OUTER JOIN
    fact_orcado o
ON
    v.id_loja = o.id_loja AND v.id_dia = o.id_dia AND v.id_producto = o.id_producto
LEFT JOIN
    tendencia_presente_view tp
ON
    COALESCE(v.id_loja, o.id_loja) = tp.id_loja
    AND COALESCE(v.id_dia, o.id_dia) = tp.id_dia
    AND COALESCE(v.id_producto, o.id_producto) = tp.id_producto;
          """)

display(df_kpis_vendas)

# COMMAND ----------

df_kpis_vendas.createOrReplaceTempView("kpis_vendas_view")

# COMMAND ----------

df_kpis_vendas_consolidado = spark.sql("""
SELECT
t.cod_mes, 
t.cod_trimestre,
t.cod_semestre,
t.cod_ano,
sum(k.cant_vendida) as total_quantidade_vendida,
sum(k.valor_venta) as total_valor_venda,
sum(k.costo_venta) as total_custo_venda,
sum(k.quantidade_vendida_orcado) as total_quantidade_vendida_orcado,
sum(k.valor_venda_orcado) as total_valor_venda_orcado,
sum(k.custo_venda_orcado) as total_custo_venda_orcado,
sum(k.quantidade_vendida_tend) as total_quantidade_vendida_tend,
sum(k.valor_venda_tend) as total_valor_venda_tend,
sum(k.custo_venda_tend) as total_custo_venda_tend
FROM kpis_vendas_view k
INNER JOIN dim_tiempo t ON k.id_dia = t.id_dia
INNER JOIN dim_producto p ON k.id_producto = p.id_producto
--WHERE k.id_loja = 10 AND p.cod_sector = 'Bebidas'
GROUP BY t.cod_mes, t.cod_trimestre, t.cod_semestre, t.cod_ano
          """)

display(df_kpis_vendas_consolidado.orderBy("cod_mes"))

# COMMAND ----------

df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn('kpi_quantidade_real_orcado',f.col('total_quantidade_vendida') / f.col('total_quantidade_vendida_orcado'))\
                                .withColumn('kpi_valor_venda_real_orcado',f.col('total_valor_venda') / f.col('total_valor_venda_orcado'))\
                                .withColumn('kpi_quantidade_tendencia_orcado',f.col('total_quantidade_vendida_tend') / f.col('total_quantidade_vendida_orcado'))\
                                .withColumn('kpi_valor__venda_tendencia_orcado',f.col('total_valor_venda_tend') / f.col('total_valor_venda_orcado'))

display(df_kpis_vendas_consolidado)

# COMMAND ----------

df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn(
    "KPI_Quantidade_Como_Estou",
    f.when(f.col("kpi_quantidade_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_quantidade_real_orcado") > 0.9) & (f.col("kpi_quantidade_real_orcado") < 1.1), 0)
     .when(f.col("kpi_quantidade_real_orcado") > 1.1, 1)
)

df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn(
    "KPI_Quantidade_Como_Estarei",
    f.when(f.col("kpi_quantidade_tendencia_orcado") <= 0.9, -1)
     .when((f.col("kpi_quantidade_tendencia_orcado") > 0.9) & (f.col("kpi_quantidade_tendencia_orcado") < 1.1), 0)
     .when(f.col("kpi_quantidade_tendencia_orcado") > 1.1, 1)
)

# COMMAND ----------

df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn(
    "KPI_Valor_da_Venda_Como_Estou",
    f.when(f.col("kpi_valor_venda_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_valor_venda_real_orcado") > 0.9) & (f.col("kpi_valor_venda_real_orcado") < 1.1), 0)
     .when(f.col("kpi_valor_venda_real_orcado") > 1.1, 1)
)
df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn(
    "KPI_Valor_da_Venda_Como_Estarei",
    f.when(f.col("kpi_valor__venda_tendencia_orcado") <= 0.9, -1)
     .when((f.col("kpi_valor__venda_tendencia_orcado") > 0.9) & (f.col("kpi_valor__venda_tendencia_orcado") < 1.1), 0)
     .when(f.col("kpi_valor__venda_tendencia_orcado") > 1.1, 1)
)

# COMMAND ----------

display(df_kpis_vendas_consolidado.orderBy('cod_mes'))

# COMMAND ----------

df_kpis_vendas_consolidado.write.format("delta").mode("overwrite").saveAsTable("databricks_project.dw_atacadez.kpis_vendas")

# COMMAND ----------

df_kpis_vendas_consolidado.select(f.mean('KPI_Valor_da_Venda_Como_Estou')).show()
