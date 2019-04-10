# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import operator
import time

# COMMAND ----------

df = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5'),
    ('id_cliente-3',  'cat-6, cat-7'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9'),
    ('id_cliente-9',  'cat-1'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

# COMMAND ----------

df2 = spark.createDataFrame([
    ('id_cliente-1',  'cat-1, cat-2, cat-3, cat-15'),
    ('id_cliente-2',  'cat-1, cat-4, cat-5, cat-11, cat-14'),
    ('id_cliente-3',  'cat-4, cat-14, cat-15'),
    ('id_cliente-4',  'cat-1, cat-2, cat-7, cat-10'),
    ('id_cliente-5',  'cat-8, cat-10, cat-11'),
    ('id_cliente-6',  'cat-1, cat-9, cat-10, cat-11, cat-13'),
    ('id_cliente-7',  'cat-1, cat-4, cat-5, cat-10'),
    ('id_cliente-8',  'cat-7, cat-9, cat-12, cat-13, cat-14'),
    ('id_cliente-9',  'cat-2'),
    ('id_cliente-10', 'cat-1, cat-2, cat-3, cat-4, cat-5, cat-6, cat-7, cat-8, cat-10')
], ['id_cliente', 'categorias'])

# COMMAND ----------



# COMMAND ----------

def calc(dataframe):
  all_cats = []
  for l in dataframe.rdd.toLocalIterator():
    for cat in l['categorias'].split(','):
      all_cats.append(cat.strip())

  all_cats = set(all_cats)
  all_cats_ints = {} 
  for cat in all_cats:
    all_cats_ints[cat] = int(cat.split('-')[1])

  all_cats = [x for x, _ in sorted(all_cats_ints.items(), key=operator.itemgetter(1))]

  df_one_encoded = dataframe
  for cat in all_cats:
    df_one_encoded = df_one_encoded.withColumn(cat, when(col("categorias").contains(cat), lit(1)).otherwise(lit(0)))

  df_one_encoded = df_one_encoded.drop("categorias")
  return df_one_encoded


# COMMAND ----------

start = time.time()
df_one_encoded = calc(df)
df_two_encoded = calc(df2)
df_one_encoded.show()
df_two_encoded.show()
print(time.time() - start)
