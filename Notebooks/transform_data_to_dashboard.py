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

df_vendas_orcado = spark.sql("""
WITH VentasAggregated AS (
    SELECT
        id_loja,
        id_producto,
        id_dia,
        SUM(cant_vendida) AS quantidade_vendida,
        SUM(valor_venta) AS valor_venda,
        SUM(costo_venta) AS custo_venda
    FROM fact_venta
    GROUP BY id_loja, id_producto, id_dia
)

SELECT
    t.cod_mes,
    v.id_producto,
    v.id_loja,
    SUM(v.quantidade_vendida) AS quantidade_vendida,
    SUM(v.valor_venda) AS valor_venda,                  
    SUM(v.custo_venda) AS custo_venda,                 
    COALESCE(SUM(o.quantidade_vendida_orcado), 0) AS quantidade_vendida_orcado,
    COALESCE(SUM(o.valor_venda_orcado), 0) AS valor_venda_orcado,
    COALESCE(SUM(o.custo_venda_orcado), 0) AS custo_venda_orcado
FROM VentasAggregated v
LEFT JOIN fact_orcado o ON v.id_loja = o.id_loja 
                        AND v.id_producto = o.id_producto 
                        AND v.id_dia = o.id_dia 
INNER JOIN dim_producto p ON v.id_producto = p.id_producto
INNER JOIN dim_departamento d ON p.cod_sector = d.cod_sector
INNER JOIN dim_tiempo t ON v.id_dia = t.id_dia
INNER JOIN dim_empresa e ON v.id_loja = e.id_loja
--WHERE e.cod_loja = '10' AND d.desc_sector = 'Bebidas'
GROUP BY t.cod_mes, v.id_loja, v.id_producto
ORDER BY t.cod_mes, v.id_loja, v.id_producto;
    """)


# COMMAND ----------

from pyspark.sql import Window

window_spec = Window.orderBy("cod_mes")
df_vendas_orcado_tendencia = df_vendas_orcado.withColumn("valor_anterior", f.lag(f.col("quantidade_vendida"), 1).over(window_spec))

df_vendas_orcado_tendencia = df_vendas_orcado_tendencia.withColumn("quantidade_vendida_tendencia", 
                   f.when(f.col("valor_anterior").isNotNull(), 
                        f.when(f.col("quantidade_vendida") > f.col("valor_anterior"), 1)
                        .when(f.col("quantidade_vendida") < f.col("valor_anterior"), -1)
                        .otherwise(0))
                   .otherwise(0))\
                       .drop("valor_anterior")

# COMMAND ----------


window_spec = Window.orderBy("cod_mes")
df_vendas_orcado_tendencia = df_vendas_orcado_tendencia.withColumn("valor_venda_anterior", f.lag(f.col("valor_venda"), 1).over(window_spec))

df_vendas_orcado_tendencia = df_vendas_orcado_tendencia.withColumn("valor_venda_tendencia", 
                   f.when(f.col("valor_venda_anterior").isNotNull(), 
                        f.when(f.col("valor_venda") > f.col("valor_venda_anterior"), 1)
                        .when(f.col("valor_venda") < f.col("valor_venda_anterior"), -1)
                        .otherwise(0))
                   .otherwise(0))\
                       .drop("valor_venda_anterior")

# COMMAND ----------

display(df_vendas_orcado_tendencia)

# COMMAND ----------

df_kpis_vendas_consolidado = df_vendas_orcado_tendencia.withColumn('kpi_quantidade_real_orcado',f.col('quantidade_vendida') / f.col('quantidade_vendida_orcado'))\
                                .withColumn('kpi_valor_venda_real_orcado',f.col('valor_venda') / f.col('valor_venda_orcado'))

display(df_kpis_vendas_consolidado)

# COMMAND ----------

df_kpis_vendas_consolidado = df_kpis_vendas_consolidado.withColumn(
    "quantidade_vendida_status",
    f.when(f.col("kpi_quantidade_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_quantidade_real_orcado") > 0.9) & (f.col("kpi_quantidade_real_orcado") < 1.1), 0)
     .when(f.col("kpi_quantidade_real_orcado") > 1.1, 1)
)\
.withColumn(
    "valor_venda_status",
    f.when(f.col("kpi_valor_venda_real_orcado") <= 0.9, -1)
     .when((f.col("kpi_valor_venda_real_orcado") > 0.9) & (f.col("kpi_valor_venda_real_orcado") < 1.1), 0)
     .when(f.col("kpi_valor_venda_real_orcado") > 1.1, 1))

# COMMAND ----------

display(df_kpis_vendas_consolidado)

# COMMAND ----------

df_kpis_vendas_consolidado.write.format("delta").mode("overwrite").saveAsTable("databricks_project.dw_atacadez.kpis_vendas")

# COMMAND ----------

spark.sql("""
MERGE INTO dim_tiempo_consolidado AS target
USING (
    SELECT
        cod_mes,
        desc_mes,
        cod_trimestre,
        desc_trimestre,
        cod_semestre,
        desc_semestre,
        cod_ano
    FROM
        dim_tiempo
    GROUP BY
        cod_mes,
        desc_mes,
        cod_trimestre,
        desc_trimestre,
        cod_semestre,
        desc_semestre,
        cod_ano
) AS source
ON target.cod_mes = source.cod_mes  -- o cualquier otra clave que defina la relación
WHEN MATCHED THEN
    UPDATE SET
        target.desc_mes = source.desc_mes,
        target.cod_trimestre = source.cod_trimestre,
        target.desc_trimestre = source.desc_trimestre,
        target.cod_semestre = source.cod_semestre,
        target.desc_semestre = source.desc_semestre,
        target.cod_ano = source.cod_ano
WHEN NOT MATCHED THEN
    INSERT (cod_mes, desc_mes, cod_trimestre, desc_trimestre, cod_semestre, desc_semestre, cod_ano)
    VALUES (source.cod_mes, source.desc_mes, source.cod_trimestre, source.desc_trimestre, source.cod_semestre, source.desc_semestre, source.cod_ano);

          """)
