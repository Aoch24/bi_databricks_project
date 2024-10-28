# Databricks notebook source
# MAGIC %md
# MAGIC # Carregando a Dimensao Empresa

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

path_silver = '/mnt/bi_project/silver'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Carregando e transformando a capa silver

# COMMAND ----------

df_empresa_silver = spark.read.format("delta").load(f"{path_silver}/tbl_empresa")
df_empresa_silver = df_empresa_silver.select('cod_loja',
                                            'desc_loja', 
                                            f.when((f.col("tamanho_loja") > 0) & (f.col("tamanho_loja") <= 6000), "Supermercados")
                                            .when((f.col("tamanho_loja") > 6000) & (f.col("tamanho_loja") < 12001), "Hipermercados")
                                            .when(f.col("tamanho_loja") > 12000, "Megamercados")
                                            .otherwise("Não definido").alias("attr_tipo_loja").cast("string"),
                                            'cod_empresa',
                                            'desc_empresa')
display(df_empresa_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Carregando o catalogo

# COMMAND ----------

df_empresa_gold = spark.table("databricks_project.dw_atacadez.dim_empresa")
display(df_empresa_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.Validando e atualizando os dados existentes

# COMMAND ----------

df_empresa_existentes = df_empresa_silver.join(df_empresa_gold,
                                               df_empresa_silver.cod_loja == df_empresa_gold.cod_loja,
                                               "inner")\
                                                .select("id_loja",
                                                        df_empresa_silver['*'])
display(df_empresa_existentes)

# COMMAND ----------

df_empresa_existentes_clean = df_empresa_existentes.dropDuplicates(["cod_loja"])
df_empresa_existentes_clean.createOrReplaceTempView("empresa_existentes_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_empresa AS gold
USING empresa_existentes_view AS src
ON gold.cod_loja = src.cod_loja
WHEN MATCHED THEN 
  UPDATE SET 
    gold.desc_loja = src.desc_loja,
    gold.attr_tipo_loja = src.attr_tipo_loja,
    gold.cod_empresa = src.cod_empresa,
    gold.desc_empresa = src.desc_empresa
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.Validando e inserindo os dados novos

# COMMAND ----------

max_id_empresa = df_empresa_gold.agg(f.max("id_loja")).collect()[0][0]
next_id_empresa = 1 if max_id_empresa is None else max_id_empresa + 1
df_empresa_novos = df_empresa_silver.join(df_empresa_gold, 
                                         df_empresa_silver.cod_loja == df_empresa_gold.cod_loja,
                                         "left_anti")
df_empresa_novos = df_empresa_novos.select(
                (f.monotonically_increasing_id() + next_id_empresa).alias("id_loja").cast('int'),                                  
                df_empresa_novos['*'])

display(df_empresa_novos)

# COMMAND ----------

df_empresa_novos.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_empresa")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a Dimensao Cliente

# COMMAND ----------

df_clientes_silver = spark.read.format("delta").load(f"{path_silver}/tbl_clientes")
df_clientes_silver = df_clientes_silver\
                                    .select(f.col("cod_cliente").cast("string"),
                                            "desc_cliente",
                                            f.when((f.col("faturamento") > 0) & (f.col("faturamento") <= 400000), "Pequenos")
                                            .when((f.col("faturamento") > 400000) & (f.col("faturamento") < 1000000), "Médios")
                                            .when(f.col("faturamento") > 1000000, "Grandes")
                                            .otherwise("Não definido").alias("attr_tamanho_cliente"),
                                            f.col('desc_cidade').alias("cod_cidade"),
                                            'desc_cidade',
                                            "cod_estado",
                                            "desc_estado",
                                            f.col("desc_regiao").alias("cod_region"),
                                            f.col("desc_regiao").alias("desc_region"),
                                            f.col("desc_segmento").alias("cod_segmento"),
                                            'desc_segmento'
                                            )
display(df_clientes_silver)

# COMMAND ----------

df_clientes_gold = spark.table("databricks_project.dw_atacadez.dim_cliente")
display(df_clientes_gold)

# COMMAND ----------

df_clientes_existentes = df_clientes_silver.join(df_clientes_gold,
                                               df_clientes_silver.cod_cliente == df_clientes_gold.cod_cliente,
                                               "inner")\
                                                .select("id_cliente",
                                                        df_clientes_silver['*'])
display(df_clientes_existentes)

# COMMAND ----------

df_clientes_existentes_clean = df_clientes_existentes.dropDuplicates(["cod_cliente"])
df_clientes_existentes_clean.createOrReplaceTempView("clientes_existentes_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_cliente AS gold
USING clientes_existentes_view AS src
ON gold.cod_cliente = src.cod_cliente
WHEN MATCHED THEN 
  UPDATE SET 
    gold.desc_cliente = src.desc_cliente,
    gold.attr_tamanho_cliente = src.attr_tamanho_cliente,
    gold.cod_cidade = src.cod_cidade,
    gold.desc_cidade = src.desc_cidade,
    gold.cod_estado = src.cod_estado,
    gold.desc_estado = src.desc_estado,
    gold.cod_region = src.cod_region,
    gold.desc_region = src.desc_region,
    gold.cod_segmento = src.cod_segmento,
    gold.desc_segmento = src.desc_segmento
    """)

# COMMAND ----------

max_id_cliente = df_clientes_gold.agg(f.max("id_cliente")).collect()[0][0]
next_id_cliente = 1 if max_id_cliente is None else max_id_cliente + 1
df_clientes_novos = df_clientes_silver.join(df_clientes_gold, 
                                         df_clientes_silver.cod_cliente == df_clientes_gold.cod_cliente,
                                         "left_anti")
df_clientes_novos = df_clientes_novos.select(
              (f.monotonically_increasing_id() + next_id_cliente).alias("id_cliente").cast('int'),                                  
              df_clientes_novos['*'])

display(df_clientes_novos)


# COMMAND ----------

df_clientes_novos.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_cliente")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a Dimensao Fornecedores

# COMMAND ----------

df_fornecedores_silver = spark.read.format("delta").load(f"{path_silver}/tbl_produtos")
df_fornecedores_silver = df_fornecedores_silver.select("cod_fornecedor", "desc_fornecedor")\
                                                .distinct().orderBy("cod_fornecedor",'desc_fornecedor')
df_fornecedores_silver = df_fornecedores_silver.dropDuplicates(["cod_fornecedor"])

display(df_fornecedores_silver)

# COMMAND ----------

df_fornecedores_gold = spark.table("databricks_project.dw_atacadez.dim_fornecedor")
display(df_fornecedores_gold)

# COMMAND ----------

df_fornecedores_existentes = df_fornecedores_silver.join(df_fornecedores_gold,
                                               df_fornecedores_silver.cod_fornecedor == df_fornecedores_gold.cod_fornecedor,
                                               "inner")\
                                                .select(df_fornecedores_silver['*'])
display(df_fornecedores_existentes)

# COMMAND ----------

df_fornecedores_existentes_clean = df_fornecedores_existentes.dropDuplicates(["cod_fornecedor"])
df_fornecedores_existentes_clean.createOrReplaceTempView("fornecedores_existentes_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_fornecedor AS gold
USING fornecedores_existentes_view AS src
ON gold.cod_fornecedor = src.cod_fornecedor
WHEN MATCHED THEN 
  UPDATE SET 
    gold.desc_fornecedor = src.desc_fornecedor
    """)

# COMMAND ----------

df_fornecedores_novos = df_fornecedores_silver.join(df_fornecedores_gold, 
                                         df_fornecedores_silver.cod_fornecedor == df_fornecedores_gold.cod_fornecedor,
                                         "left_anti")
df_fornecedores_novos = df_fornecedores_novos.select('*')

display(df_fornecedores_novos)


# COMMAND ----------

df_fornecedores_novos.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_fornecedor")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a Dimensao Departamento

# COMMAND ----------

df_departamentos_silver = spark.read.format("delta").load(f"{path_silver}/tbl_produtos")
df_departamentos_silver = df_departamentos_silver.select(f.col("desc_departamento").alias("cod_sector"),
                                                         f.col("desc_departamento").alias("desc_sector"))\
                                                .distinct().orderBy("desc_departamento")

display(df_departamentos_silver)

# COMMAND ----------

df_departamentos_gold = spark.table("databricks_project.dw_atacadez.dim_departamento")
display(df_departamentos_gold)

# COMMAND ----------

df_departamentos_existentes = df_departamentos_silver.join(df_departamentos_gold,
                                               df_departamentos_silver.cod_sector == df_departamentos_gold.cod_sector,
                                               "inner")\
                                                .select(df_departamentos_silver['*'])
display(df_departamentos_existentes)

# COMMAND ----------

df_departamentos_existentes_clean = df_departamentos_existentes.dropDuplicates(["cod_sector"])
df_departamentos_existentes_clean.createOrReplaceTempView("departamentos_existentes_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_departamento AS gold
USING departamentos_existentes_view AS src
ON gold.cod_sector = src.cod_sector
WHEN MATCHED THEN 
  UPDATE SET 
    gold.desc_sector = src.desc_sector
    """)

# COMMAND ----------

df_departamentos_novos = df_departamentos_silver.join(df_departamentos_gold, 
                                         df_departamentos_silver.cod_sector == df_departamentos_gold.cod_sector,
                                         "left_anti")
df_departamentos_novos = df_departamentos_novos.select(df_departamentos_novos['*']).orderBy('cod_sector')

display(df_departamentos_novos)

# COMMAND ----------

df_departamentos_novos.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_departamento")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a Dimensao Produto

# COMMAND ----------

df_produtos_silver = spark.read.format("delta").load(f"{path_silver}/tbl_produtos")
df_produtos_silver = df_produtos_silver.select('cod_fornecedor',
                                                f.col("desc_departamento").alias("cod_sector"),
                                                f.col("cod_produto").alias("cod_producto").cast("string"),
                                                f.col("desc_produto").alias("desc_producto"),
                                                f.col("atr_unidade_medida").alias("attr_unid_medida"))\
                                                .distinct().orderBy("cod_produto")

display(df_produtos_silver)

# COMMAND ----------

df_produtos_gold = spark.table("databricks_project.dw_atacadez.dim_producto")
display(df_produtos_gold)

# COMMAND ----------

df_produtos_existentes = df_produtos_silver.join(df_produtos_gold,
                                               df_produtos_silver.cod_producto == df_produtos_gold.cod_producto,
                                               "inner")\
                                                .select("id_producto",
                                                        df_produtos_silver['*'])
display(df_produtos_existentes)

# COMMAND ----------

df_produtos_existentes_clean = df_produtos_existentes.dropDuplicates(["cod_producto"])
df_produtos_existentes_clean.createOrReplaceTempView("produtos_existentes_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_producto AS gold
USING produtos_existentes_view AS src
ON gold.cod_producto = src.cod_producto
WHEN MATCHED THEN 
  UPDATE SET 
    gold.cod_fornecedor = src.cod_fornecedor,
    gold.cod_sector = src.cod_sector,
    gold.desc_producto = src.desc_producto,
    gold.attr_unid_medida = src.attr_unid_medida
    """)

# COMMAND ----------

max_id_produto = df_produtos_gold.agg(f.max("id_producto")).collect()[0][0]
next_id_produto = 1 if max_id_produto is None else max_id_produto + 1
df_produtos_novos = df_produtos_silver.join(df_produtos_gold, 
                                         df_produtos_silver.cod_producto == df_produtos_gold.cod_producto,
                                         "left_anti").orderBy('cod_producto')
df_produtos_novos = df_produtos_novos.select(
                (f.monotonically_increasing_id() + next_id_produto).alias("id_producto").cast('int'),                                  
                df_produtos_novos['*'])

display(df_produtos_novos)


# COMMAND ----------

df_produtos_novos.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_producto")

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregando a Dimensao Tempo

# COMMAND ----------

df_tempo_silver = spark.read.format("delta").load(f"{path_silver}/tbl_tempo")
spark.conf.set("spark.sql.session.locale", "pt-BR")
df_tempo_silver = df_tempo_silver.select(
    "cod_dia",
    f.col("data").alias("dia").cast('timestamp'),
    (f.when(f.date_format("data", "E") == 'Mon', 'segunda-feira')
     .when(f.date_format("data", "E") == 'Tue', 'terça-feira')
     .when(f.date_format("data", "E") == 'Wed', 'quarta-feira')
     .when(f.date_format("data", "E") == 'Thu', 'quinta-feira')
     .when(f.date_format("data", "E") == 'Fri', 'sexta-feira')
     .when(f.date_format("data", "E") == 'Sat', 'sábado')
     .when(f.date_format("data", "E") == 'Sun', 'domingo')).alias("attr_dia_semana"),
    (f.dayofweek("data").isin([1, 7])).cast('int').alias("attr_fin_semana").cast('boolean'),  # Fines de semana (sábado y domingo)
    f.concat(f.date_format("data", "yyyy"), f.lit("_"), f.date_format("data", "MM")).alias("cod_mes"),
    f.concat(
        f.when(f.date_format("data", "MM") == "01", "Janeiro")
         .when(f.date_format("data", "MM") == "02", "Fevereiro")
         .when(f.date_format("data", "MM") == "03", "Março")
         .when(f.date_format("data", "MM") == "04", "Abril")
         .when(f.date_format("data", "MM") == "05", "Maio")
         .when(f.date_format("data", "MM") == "06", "Junho")
         .when(f.date_format("data", "MM") == "07", "Julho")
         .when(f.date_format("data", "MM") == "08", "Agosto")
         .when(f.date_format("data", "MM") == "09", "Setembro")
         .when(f.date_format("data", "MM") == "10", "Outubro")
         .when(f.date_format("data", "MM") == "11", "Novembro")
         .when(f.date_format("data", "MM") == "12", "Dezembro"),
        f.lit(" "),
        f.date_format("data", "yyyy")
    ).alias("desc_mes"),
    f.concat(f.date_format("data", "yyyy"),f.lit("_Q0"),f.quarter("data")).alias("cod_trimestre"),
    f.concat(f.lit("0"), f.quarter("data"), f.lit(" Trimestre "), f.date_format("data", "yyyy")).alias("desc_trimestre"),
    f.concat(f.date_format('data', 'yyyy'),f.lit('_S0'),(f.floor((f.month("data") - 1) / 6) + 1)).alias("cod_semestre"),
    f.concat(f.lit('0'),f.floor((f.month("data") - 1) / 6) + 1, f.lit(" Semestre "), f.date_format("data", "yyyy")).alias("desc_semestre"),
    f.date_format("data", "yyyy").alias("cod_ano"))

display(df_tempo_silver)

# COMMAND ----------

df_tempo_gold = spark.table("databricks_project.dw_atacadez.dim_tiempo")
display(df_tempo_gold)

# COMMAND ----------

df_tempo_existente = df_tempo_silver.join(df_tempo_gold,
                                               df_tempo_silver.cod_dia == df_tempo_gold.cod_dia,
                                               "inner")\
                                                .select("id_dia",
                                                        df_tempo_silver['*'])
display(df_tempo_existente)

# COMMAND ----------

df_tempo_existente_clean = df_tempo_existente.dropDuplicates(["cod_dia"])
df_tempo_existente_clean.createOrReplaceTempView("tempo_existente_view")

# COMMAND ----------

spark.sql("""
MERGE INTO databricks_project.dw_atacadez.dim_tiempo AS gold
USING tempo_existente_view AS src
ON gold.cod_dia = src.cod_dia
WHEN MATCHED THEN 
  UPDATE SET 
    gold.dia = src.dia,
    gold.attr_dia_semana = src.attr_dia_semana,
    gold.attr_fin_semana = src.attr_fin_semana,
    gold.cod_mes = src.cod_mes,
    gold.desc_mes = src.desc_mes,
    gold.cod_trimestre = src.cod_trimestre,
    gold.desc_trimestre = src.desc_trimestre,
    gold.cod_semestre = src.cod_semestre,
    gold.desc_semestre = src.desc_semestre,
    gold.cod_ano = src.cod_ano
    """)

# COMMAND ----------

max_id_dia = df_tempo_gold.agg(f.max("id_dia")).collect()[0][0]
next_id_dia = 1 if max_id_dia is None else max_id_dia + 1
df_tempo_novo = df_tempo_silver.join(df_tempo_gold, 
                                         df_tempo_silver.cod_dia == df_tempo_gold.cod_dia,
                                         "left_anti").orderBy('cod_dia')
df_tempo_novo = df_tempo_novo.select(
                (f.monotonically_increasing_id() + next_id_dia).alias("id_dia").cast('int'),                                  
                df_tempo_novo['*'])

display(df_tempo_novo)


# COMMAND ----------

df_tempo_novo.write.format("delta").mode("append").saveAsTable("databricks_project.dw_atacadez.dim_tiempo")

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

display(df_compras_silver.groupBy(['mes_ano', 'cod_produto']).count())

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
