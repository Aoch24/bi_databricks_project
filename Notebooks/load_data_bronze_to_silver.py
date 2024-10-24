# Databricks notebook source
# MAGIC %md
# MAGIC # Empresa

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregando os dados:

# COMMAND ----------

path_bronze = '/mnt/bi_project/bronze'
path_silver = '/mnt/bi_project/silver'

# COMMAND ----------

df_empresa = spark.read.csv(f'{path_bronze}/EMPRESA.CSV', header=True, sep=';')
df_empresa.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Renomeando as colunas:

# COMMAND ----------

df_empresa = df_empresa.withColumnRenamed('LOJA', 'cod_loja') \
                        .withColumnRenamed("NOME_LOJA", "desc_loja") \
                        .withColumnRenamed("EMPRESA", "desc_empresa") \
                        .withColumnRenamed("CNPJ", "cod_empresa") \
                        .withColumnRenamed("TAMANHO", "tamanho_loja")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convertendo o tipo de dados:

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

df_empresa = df_empresa.withColumn('tamanho_loja', f.col('tamanho_loja').cast('float'))
df_empresa = df_empresa.withColumn('cod_loja', f.col('tamanho_loja').cast('integer'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Adicionando colunas de metadados:

# COMMAND ----------

df_empresa = df_empresa.withColumn('arquivo_origem', f.lit('EMPRESA.CSV'))
df_empresa = df_empresa.withColumn('data_carga', f.current_timestamp())

# COMMAND ----------

display(df_empresa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvando o arquivo delta

# COMMAND ----------

df_empresa.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_empresa")

# COMMAND ----------

display(dbutils.fs.ls(f"{path_silver}/tbl_empresa"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Clientes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lendo os dados da planilha:

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/bi_project/bronze/'))

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import pandas as pd

df_clientes = pd.read_excel(f'/dbfs{path_bronze}/CADASTRO_DE_CLIENTES.xlsx')
df_clientes = spark.createDataFrame(df_clientes)
display(df_clientes)

# COMMAND ----------

df_estados = spark.read.csv(f"{path_bronze}/REGIOES_DOS_ESTADOS.csv", sep=';', header=True)
display(df_estados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tranformacoes

# COMMAND ----------

df_estados = df_estados\
    .withColumnRenamed("ESTADO ABREVIA��O", "cod_estado")\
    .withColumnRenamed("ESTADO", "desc_estado")\
    .withColumnRenamed("REGIAO", "desc_regiao")
display(df_estados)

# COMMAND ----------

df_clientes = df_clientes\
    .withColumnRenamed("CNPJ", "cod_cliente")\
    .withColumnRenamed("CLIENTE", "desc_cliente")\
    .withColumnRenamed("ENDERECO", "endereco")\
    .withColumnRenamed("SEGMENTO", "desc_segmento")\
    .withColumnRenamed("FATURAMENTO", "faturamento")

display(df_clientes)

# COMMAND ----------

df_clientes = df_clientes.withColumn("desc_cidade", f.regexp_extract(f.col("endereco"),r"-\s([A-Za-zÀ-ÖØ-ÿ\s]+)\s-\s", 1))
df_clientes = df_clientes.withColumn("cod_estado", f.regexp_extract(f.col("endereco"),r"\b([A-Z]{2})\b", 1))
display(df_clientes)

# COMMAND ----------

df_clientes = df_clientes.join(df_estados, df_clientes.cod_estado == df_estados.cod_estado, "left")\
                .select(df_clientes.cod_cliente,df_clientes.desc_cliente, df_clientes.endereco, df_clientes.desc_segmento,
                        df_clientes.faturamento, df_clientes.desc_cidade, df_clientes.cod_estado, df_estados.desc_estado,
                        df_estados.desc_regiao)
df_clientes = df_clientes.withColumn("arquivo_origem", f.lit("CADASTRO DE CLIENTES.xlsx")).withColumn("data_carga", f.current_timestamp())

display(df_clientes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Salvando o arquivo na camada silver

# COMMAND ----------

df_clientes.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_clientes")

# COMMAND ----------

display(dbutils.fs.ls(f"{path_silver}/tbl_clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Produtos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Formatando o arquivo

# COMMAND ----------

import json
with open(f"/dbfs/{path_bronze}/PRODUTOS.JSON", "r", encoding="latin1") as f:
    try:
        data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Erro no formato JSON: {e}")
        exit(1)

# Guardar el contenido corregido y formateado en un nuevo archivo JSON
with open(f"/dbfs/{path_bronze}/PRODUTOS_clean.JSON", "w", encoding="utf-8") as f_clean:
    # Escribir con indentación para mejorar la legibilidad
    json.dump(data, f_clean, ensure_ascii=False, indent=4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.Criando o Dataframe Produtos

# COMMAND ----------

df_produtos = spark.read.option("multiline", "true").json(f"{path_bronze}/PRODUTOS_clean.JSON")
display(df_produtos)

# COMMAND ----------

df_fornecedores = pd.read_xml(f"/dbfs/{path_bronze}/FORNECEDORES.XML", xpath=".//Produto", parser='etree', encoding='latin1')
df_fornecedores = spark.createDataFrame(df_fornecedores)
display(df_fornecedores)

# COMMAND ----------

df_produtos = df_produtos.join(df_fornecedores, df_produtos.SKU == df_fornecedores.SKU, how='inner')\
                .select(df_produtos.DEPARTAMENTO, df_produtos.DESCRITOR, df_produtos.SKU, df_produtos.UNIDADE,
                        df_fornecedores.CNPJ, df_fornecedores.FORNECEDOR)
display(df_produtos)

# COMMAND ----------

df_produtos = df_produtos.withColumn('CNPJ', f.col('CNPJ').cast('string'))\

df_produtos = df_produtos.select(df_produtos.SKU.alias('cod_produto'),
                                 df_produtos.DESCRITOR.alias('desc_produto'),
                                 df_produtos.DEPARTAMENTO.alias('desc_departamento'),
                                 df_produtos.UNIDADE.alias('atr_unidade_medida'),
                                 df_produtos.CNPJ.alias('cod_fornecedor'),
                                 df_produtos.FORNECEDOR.alias('desc_fornecedor'),
                                 f.lit('FORNECEDORES.XML e PRODUTOS.JSON').alias('arquivo_origem'),
                                 f.current_timestamp().alias('data_carga'))

# COMMAND ----------

display(df_produtos)

# COMMAND ----------

df_produtos.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_produtos")

# COMMAND ----------

display(dbutils.fs.ls(f"{path_silver}/tbl_produtos"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Notas

# COMMAND ----------

import os
import shutil

# COMMAND ----------

path_in = f'{path_bronze}/mov/'
path_out = f'{path_bronze}/mov/consumidos'

dfs = []

for file in dbutils.fs.ls(path_in):
    filename = file.name
    if filename.startswith('Movimento_Notas') and filename.endswith('.csv'):
        df_temp = spark.read.csv(os.path.join(path_in, filename), header=True, inferSchema=True, sep=';')
        df_temp = df_temp.withColumn('arquivo_origem', f.lit(filename))
        dfs.append(df_temp)

if dfs:
    df_notas = dfs[0]
    for df in dfs[1:]:
        df_notas = df_notas.union(df)

# COMMAND ----------

df_notas = df_notas.orderBy('numero')
display(df_notas)

# COMMAND ----------

df_notas = df_notas.withColumn('cpf', f.col('cpf').cast('string'))\
                    .withColumn('codigo_loja', f.col('codigo_loja').cast('string'))

# COMMAND ----------

df_notas = df_notas.select(df_notas.numero,
                                 df_notas.cpf.alias('cod_cliente'),
                                 df_notas.codigo_loja.alias('cod_loja'),
                                 df_notas.data,
                                 df_notas.arquivo_origem,
                                 f.current_timestamp().alias('data_carga'))

# COMMAND ----------

display(df_notas)

# COMMAND ----------

df_notas.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_notas")
display(dbutils.fs.ls(f"{path_silver}/tbl_notas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tempo

# COMMAND ----------

df_tempo = df_notas.select(
  f.regexp_replace('data', '-', '').alias('cod_dia'),'data', f.lit(1).alias('controle')
  ).distinct().orderBy('cod_dia')

display(df_tempo)


# COMMAND ----------

df_notas.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_tempo")
display(dbutils.fs.ls(f"{path_silver}/tbl_tempo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Movendo as notas lidas para a pasta consumidos

# COMMAND ----------

files_names_proccesed = df_notas.select('arquivo_origem').distinct().orderBy('arquivo_origem').rdd.flatMap(lambda x: x).collect()

try:
    dbutils.fs.ls(path_out)
except Exception as e:
    dbutils.fs.mkdirs(path_out)

for file in files_names_proccesed:
    file_in_path = f"{path_in}/{file}"
    file_out_path = f"{path_out}/{file}"
    try:
        dbutils.fs.ls(file_in_path)
        dbutils.fs.mv(file_in_path, file_out_path, recurse=True)
    except Exception as e:
        print('Error: {e}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Notas Items

# COMMAND ----------

dfs = []

for file in dbutils.fs.ls(path_in):
    filename = file.name
    if filename.startswith('Movimento_Itens') and filename.endswith('.csv'):
        df_temp = spark.read.csv(os.path.join(path_in, filename), header=True, inferSchema=True, sep=';')
        df_temp = df_temp.withColumn('arquivo_origem', f.lit(filename))
        dfs.append(df_temp)

if dfs:
    df_notas_items = dfs[0]
    for df in dfs[1:]:
        df_notas_items = df_notas_items.union(df)

# COMMAND ----------

df_notas_items = df_notas_items.orderBy('numero')
display(df_notas_items)

# COMMAND ----------

df_notas_items = df_notas_items.withColumn('quantidade', f.col('quantidade').cast('float'))\
                    .withColumn('preco', f.col('preco').cast('float'))

# COMMAND ----------

df_notas_items = df_notas_items.select(df_notas_items.numero,
                                 df_notas_items.codigo_produto.alias('cod_produto'),
                                 df_notas_items.quantidade,
                                 df_notas_items.preco,
                                 df_notas_items.arquivo_origem,
                                 f.current_timestamp().alias('data_carga'))

# COMMAND ----------

display(df_notas_items)

# COMMAND ----------

df_notas_items.write.format("delta").mode("overwrite").save(f"{path_silver}/tbl_notas_items")

# COMMAND ----------

display(dbutils.fs.ls(f"{path_silver}/tbl_notas_items"))

# COMMAND ----------

files_names_proccesed = df_notas_items.select('arquivo_origem').distinct().orderBy('arquivo_origem').rdd.flatMap(lambda x: x).collect()

try:
    dbutils.fs.ls(path_out)
except Exception as e:
    dbutils.fs.mkdirs(path_out)

for file in files_names_proccesed:
    file_in_path = f"{path_in}/{file}"
    file_out_path = f"{path_out}/{file}"
    try:
        dbutils.fs.ls(file_in_path)
        dbutils.fs.mv(file_in_path, file_out_path, recurse=True)
    except Exception as e:
        print('Error: {e}')

# COMMAND ----------

display(dbutils.fs.ls(f"{path_out}"))
