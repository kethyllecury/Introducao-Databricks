# Databricks notebook source
import zipfile
import os
from pyspark.sql.functions import *

# COMMAND ----------

display(dbutils.fs.ls("FileStore/tables"))

# COMMAND ----------

zip_dbfs= "dbfs:/FileStore/tables/discogs_csv.zip"
zip_local = "/tmp/discogs_csv.zip"
extract = "/tmp/discogs_csv_novo/"

dbutils.fs.cp(zip_dbfs, f"file:{zip_local}")
os.makedirs(extract, exist_ok=True)

with zipfile.ZipFile(zip_local, 'r') as zip_ref:
    zip_ref.extractall(extract)

print(f"arquivos extraídos em: {extract}")


# COMMAND ----------

csv= "file:/tmp/discogs_csv_novo/"
csv_dbfs= "dbfs:/FileStore/tables/discogs.csv"

dbutils.fs.cp(csv, csv_dbfs, recurse=True)

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/tables/discogs.csv/",header = True,inferSchema = True,sep = ",",quote='"',escape='"',multiLine=True)

# COMMAND ----------

parquet_path = "dbfs:/FileStore/tables/discogs_parquet"
df.write.mode("overwrite").parquet(parquet_path)

# COMMAND ----------

df_parquet = spark.read.parquet("dbfs:/FileStore/tables/discogs_parquet/")
display(df_parquet)


# COMMAND ----------

for c in df_parquet.columns:
    null = df_parquet.select(count(when(col(c).isNull(), c)).alias(c))
    display(null)

# COMMAND ----------

df_null = df_parquet.dropna(how ="all")
df_drop = df_null.dropDuplicates()


# COMMAND ----------

for c in df_drop.columns:
    if dict(df_drop.dtypes)[c] in ['int', 'bigint', 'double', 'float', 'decimal']:
        df_drop = df_drop.fillna({c: 0})
    else:
        df_drop = df_drop.fillna({c: "desconhecido"})

# COMMAND ----------

df_string = df_drop.withColumn("release_date", col("release_date").cast("string"))

# COMMAND ----------


df_date = df_string.withColumn("release_date",when(col("release_date") == "0", lit("1900-01-01")).otherwise(concat(col("release_date").cast("string"), lit("-01-01")))).withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))


# COMMAND ----------

for c in df_date.columns:
    df_null = df_date.select(count(when(col(c).isNull(), c)).alias(c))
    display(df_null)

# COMMAND ----------

'''
Se quiser prever release_date (ano que o disco foi lançado)

obs: aplicar engenharia de atributos 

features: genre,format,label_id
target: release_date

'''

# COMMAND ----------

df_bronze = df_parquet
df_bronze.write.mode("overwrite").parquet("dbfs:/FileStore/tables/bronze/discogs_bronze.parquet")

# COMMAND ----------

df_silver = df_string
df_silver.write.mode("overwrite").parquet("dbfs:/FileStore/tables/silver/discogs_silver.parquet")

# COMMAND ----------

df_gold = df_date
df_gold.write.mode("overwrite").parquet("dbfs:/FileStore/tables/gold/discogs_gold.parquet")


# COMMAND ----------

df_parquet_gold = spark.read.parquet("dbfs:/FileStore/tables/gold/discogs_gold.parquet")
display(df_parquet_gold)


# COMMAND ----------

display(df_parquet_gold.select("status").distinct())

# COMMAND ----------

'''

supondo que o time de analistas precise desses dados filtrados em disco para otimizar futuras consultas acessar apenas o status "Accepted"

'''

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/tables/gold/discogs_gold_divided.parquet")

# COMMAND ----------

df_parquet_gold.write.mode("overwrite").partitionBy("status").parquet("dbfs:/FileStore/tables/gold/discogs_gold_divided.parquet")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/gold/discogs_gold_divided.parquet/")) 

# COMMAND ----------

df_accepted = spark.read.parquet("dbfs:/FileStore/tables/gold/discogs_gold_divided.parquet/status=Accepted")
display(df_accepted)


# COMMAND ----------

'''

separar em tabelas fato e dimensao

'''

# COMMAND ----------

dimensao_artista = df_accepted.select("artist_id", "artist_name").distinct()
dimensao_artista.write.format("delta").mode("overwrite").saveAsTable("dimensao_artista")
display(dimensao_artista)

# COMMAND ----------

dimensao_label = df_accepted.select("label_id", "label_name", "company_name").distinct()
dimensao_label.write.format("delta").mode("overwrite").saveAsTable("dimensao_label")
display(dimensao_label)


# COMMAND ----------

discogs_fato = df_accepted.select("id","release_id","title","artist_id","label_id","format","genre","style","country","release_date","notes","master_id","video_url","company_name")

discogs_fato.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("discogs_fato")