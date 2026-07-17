# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys
sys.path.append('/Workspace/Users/rhian.souza@programmers.com.br/gov_br_atletas_analise/src/utils')

# COMMAND ----------

import bronze_utils

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_atletas_dezembro = spark.read.csv(f"/Volumes/projeto_gov/bronze/atletas/gov-csv/Bolsa Atleta Dezembro(Planilha1).csv", header=True, inferSchema=True,sep=";")

# COMMAND ----------

df_atletas_janeiro = spark.read.csv(f"/Volumes/projeto_gov/bronze/atletas/gov-csv/PDA2026-01_Bolsa Atleta(Planilha1).csv", header=True, inferSchema=True,sep=";")

# COMMAND ----------

df_atletas_fevereiro = spark.read.csv(f"/Volumes/projeto_gov/bronze/atletas/gov-csv/PDA202602_Bolsa Atleta(Planilha2).csv", header=True, inferSchema=True,sep=";")

# COMMAND ----------

df_atletas_marco = spark.read.csv(f"/Volumes/projeto_gov/bronze/atletas/gov-csv/PDA202603 - BolsaAtleta(Sheet1).csv", header=True, inferSchema=True,sep=";")

# COMMAND ----------

df_atletas_abril = spark.read.csv(f"/Volumes/projeto_gov/bronze/atletas/gov-csv/PDA202604 - BolsaAtleta(Planilha1).csv", header=True, inferSchema=True,sep=";")

# COMMAND ----------

df_atletas_dezembro = bronze_utils.transform_to_string(df_atletas_dezembro)
df_atletas_janeiro = bronze_utils.transform_to_string(df_atletas_janeiro)
df_atletas_fevereiro = bronze_utils.transform_to_string(df_atletas_fevereiro)
df_atletas_marco = bronze_utils.transform_to_string(df_atletas_marco)
df_atletas_abril = bronze_utils.transform_to_string(df_atletas_abril)

# COMMAND ----------

# MAGIC %md
# MAGIC column names to snake_case

# COMMAND ----------

df_atletas_dezembro = bronze_utils.remove_bda_chars_from_columns(df_atletas_dezembro)

# COMMAND ----------

df_atletas_janeiro = bronze_utils.remove_bda_chars_from_columns(df_atletas_janeiro)

# COMMAND ----------

df_atletas_fevereiro = bronze_utils.remove_bda_chars_from_columns(df_atletas_fevereiro)

# COMMAND ----------

df_atletas_marco = bronze_utils.remove_bda_chars_from_columns(df_atletas_marco)

# COMMAND ----------

df_atletas_abril = bronze_utils.remove_bda_chars_from_columns(df_atletas_abril)

# COMMAND ----------

# MAGIC %md
# MAGIC Salvamentos

# COMMAND ----------

df_atletas_dezembro = bronze_utils.save_to_bronze(df_atletas_dezembro, 'atletas_dezembro')

# COMMAND ----------

df_atletas_janeiro = bronze_utils.save_to_bronze(df_atletas_janeiro, 'atletas_janeiro')

# COMMAND ----------

df_atletas_fevereiro = bronze_utils.save_to_bronze(df_atletas_fevereiro, 'atletas_fevereiro')

# COMMAND ----------

df_atletas_marco = bronze_utils.save_to_bronze(df_atletas_marco, 'atletas_marco')

# COMMAND ----------

df_atletas_abril = bronze_utils.save_to_bronze(df_atletas_abril, 'atletas_abril')