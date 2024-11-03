# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a tabela de Fatos Vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Carregando a tabela de Notas

# COMMAND ----------

df_notas_silver = spark.read.format("delta").load(f"{path_silver}/tbl_notas")
df_notas_silver = df_notas_silver.select('numero',
                                            'cod_cliente',
                                            'cod_loja',
                                            f.regexp_replace('data', '-', '').alias('cod_dia'))
display(df_notas_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Carregando a tabela de Notas Items

# COMMAND ----------

df_notas_items_silver = spark.read.format("delta").load(f"{path_silver}/tbl_notas_items")
df_notas_items_silver = df_notas_items_silver.select('numero',
                                            'cod_produto',
                                            'quantidade',
                                            'preco').orderBy('numero')
display(df_notas_items_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Carregando a tabela de Compras

# COMMAND ----------

df_compras_silver = spark.read.format("delta").load(f"{path_silver}/tbl_compras")

display(df_compras_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.Fazendo a juncao das tabelas

# COMMAND ----------

df_notas_join_silver = df_notas_silver.join(df_notas_items_silver,
                                               df_notas_silver.numero == df_notas_items_silver.numero,
                                               "inner")\
                                                .select(
                                                        'cod_cliente',
                                                        'cod_loja',
                                                        'cod_dia',
                                                        'cod_produto',
                                                        'quantidade',
                                                        'preco',
                                                        f.concat(f.substring("cod_dia", 1, 4), f.lit("-"), f.substring("cod_dia", 5, 2)).alias("mes_ano"))
display(df_notas_join_silver)

# COMMAND ----------

df_vendas_silver = df_compras_silver.join(df_notas_join_silver,
                                               (df_compras_silver.cod_produto == df_notas_join_silver.cod_produto) &
                                               (df_compras_silver.mes_ano == df_notas_join_silver.mes_ano),
                                               "inner")\
                                                .select(
                                                        'cod_cliente',
                                                        'cod_loja',
                                                        'cod_dia',
                                                        df_notas_join_silver.cod_produto.alias('cod_producto'),
                                                        'quantidade',
                                                        'preco',
                                                        'preco_compra')
display(df_vendas_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## 5.Adicionando as colunas dos indicadores de valor venda e custo da venda

# COMMAND ----------

df_vendas_silver = df_vendas_silver.withColumn("valor_venda", (df_vendas_silver.quantidade * df_vendas_silver.preco).cast('float'))\
                                   .withColumn("custo_venda", (df_vendas_silver.quantidade * df_vendas_silver.preco_compra).cast('float'))

display(df_vendas_silver)

# COMMAND ----------

df_vendas_agregado = df_vendas_silver.groupBy('cod_cliente', 'cod_loja', 'cod_dia', 'cod_producto').agg(
    f.sum("quantidade").alias("cant_vendida").cast('float'),
    f.sum("valor_venda").alias("valor_venta").cast('float'),
    f.sum("custo_venda").alias("costo_venta").cast('float'))

display(df_vendas_agregado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.Fazendo o merge para a tabela de Fatos

# COMMAND ----------

spark.sql("USE databricks_project.dw_atacadez")

# COMMAND ----------

empresa_gold = 'databricks_project.dw_atacadez.dim_empresa'
cliente_gold = 'databricks_project.dw_atacadez.dim_cliente'
produto_gold = 'databricks_project.dw_atacadez.dim_producto'
tempo_gold = 'databricks_project.dw_atacadez.dim_tiempo'
#fato_vendas_gold = spark.table("databricks_project.dw_atacadez.fact_venta")

# COMMAND ----------

df_vendas_agregado.createOrReplaceTempView("vendas_silver_view")

# COMMAND ----------

# Usar MERGE para agregar nuevos registros a la tabla de ventas gold
spark.sql(f"""
MERGE INTO databricks_project.dw_atacadez.fact_venta AS gold
USING (
    SELECT 
        COALESCE(e.id_loja, 0) AS id_loja, 
        COALESCE(c.id_cliente, 0) AS id_cliente,
        COALESCE(p.id_producto, 0) AS id_producto,
        COALESCE(t.id_dia, 0) AS id_dia,
        f.cant_vendida,
        f.valor_venta,
        f.costo_venta
    FROM vendas_silver_view f
    LEFT JOIN {empresa_gold} e ON f.cod_loja = e.cod_loja
    LEFT JOIN {cliente_gold} c ON f.cod_cliente = c.cod_cliente
    LEFT JOIN {produto_gold} p ON f.cod_producto = p.cod_producto
    LEFT JOIN {tempo_gold} t ON f.cod_dia = t.cod_dia

) AS new_data 
ON gold.id_loja = new_data.id_loja 
AND gold.id_cliente = new_data.id_cliente 
AND gold.id_producto = new_data.id_producto 
AND gold.id_dia = new_data.id_dia
WHEN MATCHED THEN 
    UPDATE SET 
        gold.cant_vendida = new_data.cant_vendida
        , gold.valor_venta = new_data.valor_venta
        , gold.costo_venta = new_data.costo_venta
WHEN NOT MATCHED THEN 
    INSERT (id_loja, id_cliente, id_producto, id_dia, cant_vendida, valor_venta, costo_venta) 
    VALUES (new_data.id_loja, new_data.id_cliente, new_data.id_producto, new_data.id_dia, new_data.cant_vendida, new_data.valor_venta, new_data.costo_venta)
""")


# COMMAND ----------

fato_vendas_gold = spark.table("databricks_project.dw_atacadez.fact_venta")
display(fato_vendas_gold.orderBy('id_dia'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a tabela de Fatos Orcamento

# COMMAND ----------

spark.sql("USE databricks_project.dw_atacadez")

df_vendas_realizadas = spark.sql("""
SELECT 
dim_empresa.id_loja, 
dim_producto.id_producto,
dim_producto.cod_producto, 
dim_tiempo.id_dia, 
dim_empresa.cod_loja, 
dim_departamento.desc_sector, 
dim_tiempo.cod_mes, 
SUM(fact_venta.cant_vendida) as quantidade_vendida, 
SUM(fact_venta.valor_venta) as valor_venda, 
SUM(fact_venta.costo_venta) as custo_venda
FROM     dim_departamento INNER JOIN
                  dim_producto ON dim_departamento.cod_sector = dim_producto.cod_sector INNER JOIN
                  fact_venta ON dim_producto.id_producto = fact_venta.id_producto INNER JOIN
                  dim_empresa ON fact_venta.id_loja = dim_empresa.id_loja INNER JOIN
                  dim_tiempo ON fact_venta.id_dia = dim_tiempo.id_dia
GROUP BY 
dim_empresa.id_loja, 
dim_producto.id_producto,
dim_producto.cod_producto,
dim_tiempo.id_dia, 
dim_empresa.cod_loja, 
dim_departamento.desc_sector, 
dim_tiempo.cod_mes
                                 """)

display(df_vendas_realizadas)

# COMMAND ----------

df_vendas_consolidadas = spark.sql("""
SELECT dim_empresa.cod_loja, 
dim_departamento.desc_sector, 
dim_tiempo.cod_mes, 
SUM(fact_venta.cant_vendida) AS quantidade_vendida_total, 
SUM(fact_venta.valor_venta) AS valor_venda_total, 
SUM(fact_venta.costo_venta) AS custo_venda_total
FROM     dim_departamento INNER JOIN
                  dim_producto ON dim_departamento.cod_sector = dim_producto.cod_sector INNER JOIN
                  fact_venta ON dim_producto.id_producto = fact_venta.id_producto INNER JOIN
                  dim_empresa ON fact_venta.id_loja = dim_empresa.id_loja INNER JOIN
                  dim_tiempo ON fact_venta.id_dia = dim_tiempo.id_dia
GROUP BY
dim_empresa.cod_loja, 
dim_departamento.desc_sector, 
dim_tiempo.cod_mes
                                   """)

display(df_vendas_consolidadas)

# COMMAND ----------

df_orcamento = spark.read.format("delta").load(f"{path_silver}/tbl_orcamento_consolidado")
display(df_orcamento)

# COMMAND ----------

df_vendas_realizadas_consolidadas = df_vendas_realizadas.join(df_vendas_consolidadas,
                                               (df_vendas_realizadas.cod_loja == df_vendas_consolidadas.cod_loja)
                                               & (df_vendas_realizadas.cod_mes == df_vendas_consolidadas.cod_mes)
                                               & (df_vendas_realizadas.desc_sector == df_vendas_consolidadas.desc_sector),
                                               "inner")\
                                                .drop(df_vendas_consolidadas.cod_loja,
                                                                df_vendas_consolidadas.cod_mes, 
                                                                df_vendas_consolidadas.desc_sector)\
                                                .orderBy(df_vendas_realizadas.cod_loja,
                                                                df_vendas_realizadas.cod_mes, 
                                                                df_vendas_realizadas.desc_sector)

display(df_vendas_realizadas_consolidadas)

# COMMAND ----------

df_vendas_realizadas_consolidadas = df_vendas_realizadas_consolidadas\
    .withColumn("quantidade_vendida_porcentual_distribuicao", f.col("quantidade_vendida") / f.col("quantidade_vendida_total"))\
    .withColumn("valor_venda_porcentual_distribuicao", f.col("valor_venda") / f.col("valor_venda_total"))\
    .withColumn("custo_venda_porcentual_distribuicao", f.col("custo_venda") / f.col("custo_venda_total"))

# COMMAND ----------

df_vendas_join_realizadas_orcadas = df_vendas_realizadas_consolidadas.join(df_orcamento,
                                               (df_vendas_realizadas_consolidadas.cod_loja == df_orcamento.cod_loja)
                                               & (df_vendas_realizadas_consolidadas.cod_mes == df_orcamento.cod_mes)
                                               & (df_vendas_realizadas_consolidadas.desc_sector == df_orcamento.desc_departamento),
                                               "inner")\
                                                .drop(df_orcamento.cod_loja,
                                                                df_orcamento.cod_mes, 
                                                                df_orcamento.desc_departamento,
                                                                df_orcamento.arquivo_origem,
                                                                df_orcamento.data_carga)\
                                                .orderBy(df_vendas_realizadas_consolidadas.cod_loja,
                                                                df_vendas_realizadas_consolidadas.cod_mes, 
                                                                df_vendas_realizadas_consolidadas.desc_sector)

display(df_vendas_join_realizadas_orcadas)

# COMMAND ----------

df_vendas_join_realizadas_orcadas = df_vendas_join_realizadas_orcadas\
    .withColumn("quantidade_vendida_orcada_distribuida", f.col("quantidade_vendida_porcentual_distribuicao") * f.col("quantidade_vendida_orc"))\
    .withColumn("valor_venda_orcado_distribuida", f.col("valor_venda_porcentual_distribuicao") * f.col("valor_venda_orc"))\
    .withColumn("custo_venda_orcado_distribuido", f.col("custo_venda_porcentual_distribuicao") * f.col("custo_venda_orc"))

display(df_vendas_join_realizadas_orcadas)

# COMMAND ----------

df_fact_orcado = df_vendas_join_realizadas_orcadas.select('id_loja',
                                                          f.lit(0).alias("id_cliente"),
                                                          'id_producto',
                                                          'id_dia',
                                                          f.col('quantidade_vendida_orcada_distribuida').alias('quantidade_vendida_orcado').cast("float"),
                                                          f.col('valor_venda_orcado_distribuida').alias('valor_venda_orcado').cast("float"),
                                                          f.col('custo_venda_orcado_distribuido').alias('custo_venda_orcado').cast("float"))

display(df_fact_orcado)

# COMMAND ----------

df_fact_orcado.write.format("delta").mode("overwrite").saveAsTable("databricks_project.dw_atacadez.fact_orcado")
