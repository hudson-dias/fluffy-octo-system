# coding=utf-8

# importa o PySpark e inicializa o SparkContext e o SparkSession
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


sc = SparkContext('local')
spark = SparkSession(sc)

# carrega os dataframes d1 e df2 a partir de arquivos CSV
df1 = spark.read.csv('\payload01.csv', header=True, encoding='utf-8')
df2 = spark.read.csv('\payload02.csv', header=True, encoding='utf-8')

# suponha que você tenha dois dataframes: df1 e df2

# cria um dataframe resultado com as colunas que você deseja incluir
df_result = df1.select(df1.Id, df1.Nome, df1.Numero)

# junta df1 e df2 pelo Id
df_result = df_result.join(df2, on=df1.Id == df2.Id, how='inner')

# valida se a coluna Endereco do df1 existe e é igual ao valor da coluna Documento do df2
df_result = df_result.where(F.col("Endereco").isNotNull() & F.col("Endereco") == F.col("Documento"))

# seleciona a coluna Transacao e Data_Fim do df2 e adiciona ao dataframe resultado
df_result = df_result.select(df_result['*'], df2.Transacao, df2.Data_Fim)

# renomeia o dataframe resultado para json_03
df_result = df_result.withColumnRenamed('*', 'json_03')

# salva o dataframe resultado como um arquivo CSV, incluindo o cabeçalho
df_result.write.csv('/pyspark_test/output/result.csv', header=True, encoding='utf-8')

print(df_result)