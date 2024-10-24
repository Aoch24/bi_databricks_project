# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Criando o schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DW_ATACADEZ;
# MAGIC USE DW_ATACADEZ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.Criando as dimensiones

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_cliente (
# MAGIC     id_cliente INT,
# MAGIC     cod_cliente STRING NOT NULL,
# MAGIC     desc_cliente STRING NOT NULL,
# MAGIC     attr_tamanho_cliente STRING NOT NULL,
# MAGIC     cod_cidade STRING NOT NULL,
# MAGIC     desc_cidade STRING NOT NULL,
# MAGIC     cod_estado STRING NOT NULL,
# MAGIC     desc_estado STRING NOT NULL,
# MAGIC     cod_region STRING NOT NULL,
# MAGIC     desc_region STRING NOT NULL,
# MAGIC     cod_segmento STRING NOT NULL,
# MAGIC     desc_segmento STRING NOT NULL,
# MAGIC     PRIMARY KEY (id_cliente)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_departamento (
# MAGIC     cod_sector STRING NOT NULL,
# MAGIC     desc_sector STRING NOT NULL,
# MAGIC     PRIMARY KEY (cod_sector)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_empresa (
# MAGIC     id_loja INT,
# MAGIC     cod_loja STRING NOT NULL,
# MAGIC     desc_loja STRING NOT NULL,
# MAGIC     attr_tipo_loja STRING NOT NULL,
# MAGIC     cod_empresa STRING NOT NULL,
# MAGIC     desc_empresa STRING NOT NULL,
# MAGIC     PRIMARY KEY (id_loja)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_fornecedor (
# MAGIC     cod_fornecedor STRING NOT NULL,
# MAGIC     desc_fornecedor STRING NOT NULL,
# MAGIC     PRIMARY KEY (cod_fornecedor)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_producto (
# MAGIC     id_producto INT,
# MAGIC     cod_fornecedor STRING NOT NULL,
# MAGIC     cod_sector STRING NOT NULL,
# MAGIC     cod_producto STRING NOT NULL,
# MAGIC     desc_producto STRING NOT NULL,
# MAGIC     attr_unid_medida STRING NOT NULL,
# MAGIC     PRIMARY KEY (id_producto),
# MAGIC     FOREIGN KEY (cod_fornecedor) REFERENCES dim_fornecedor(cod_fornecedor),
# MAGIC     FOREIGN KEY (cod_sector) REFERENCES dim_departamento(cod_sector)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_tiempo (
# MAGIC     id_dia INT,
# MAGIC     cod_dia STRING NOT NULL,
# MAGIC     dia TIMESTAMP NOT NULL,
# MAGIC     attr_dia_semana STRING NOT NULL,
# MAGIC     attr_fin_semana BOOLEAN NOT NULL,
# MAGIC     cod_mes STRING NOT NULL,
# MAGIC     desc_mes STRING NOT NULL,
# MAGIC     cod_trimestre STRING NOT NULL,
# MAGIC     desc_trimestre STRING NOT NULL,
# MAGIC     cod_semestre STRING NOT NULL,
# MAGIC     desc_semestre STRING NOT NULL,
# MAGIC     cod_ano STRING NOT NULL,
# MAGIC     PRIMARY KEY (id_dia)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.Criando a tabela de Fatos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS fact_venta (
# MAGIC     id_loja INT NOT NULL,
# MAGIC     id_cliente INT NOT NULL,
# MAGIC     id_producto INT NOT NULL,
# MAGIC     id_dia INT NOT NULL,
# MAGIC     cant_vendida FLOAT NOT NULL,
# MAGIC     valor_venta FLOAT NOT NULL,
# MAGIC     costo_venta FLOAT NOT NULL,
# MAGIC     PRIMARY KEY (id_loja, id_cliente, id_producto, id_dia),
# MAGIC     FOREIGN KEY (id_loja) REFERENCES dim_empresa(id_loja),
# MAGIC     FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
# MAGIC     FOREIGN KEY (id_producto) REFERENCES dim_producto(id_producto),
# MAGIC     FOREIGN KEY (id_dia) REFERENCES dim_tiempo(id_dia)
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Inserindo os dados ND

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dim_cliente VALUES
# MAGIC (0, 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND');
# MAGIC INSERT INTO dim_departamento VALUES
# MAGIC ('ND', 'ND');
# MAGIC INSERT INTO dim_fornecedor VALUES
# MAGIC ('ND', 'ND');
# MAGIC INSERT INTO dim_empresa VALUES
# MAGIC (0, 'ND', 'ND', 'ND', 'ND', 'ND');
# MAGIC INSERT INTO dim_producto VALUES
# MAGIC (0, 'ND', 'ND', 'ND', 'ND', 'ND');
# MAGIC INSERT INTO dim_tiempo VALUES
# MAGIC (0, 'ND', '1900-01-01 00:00:00', 'ND', false, 'ND', 'ND', 'ND', 'ND', 'ND', 'ND', 'ND');
