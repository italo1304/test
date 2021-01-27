from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .enableHiveSupport() \
    .getOrCreate()


df_vuelos = spark.table("vuelos").na.drop()

df_salidas = (df_vuelos.groupBy('origen').count()).sort(f.col("count").desc()).limit(1)
df_llegadas = (df_vuelos.groupBy('destino').count()).sort(f.col("count").desc()).limit(1)

print("País top salida vuelos")
print(df_salidas.show())
print("País top llegada vuelos")
print(df_llegadas.show())

df_vuelo_fecha = spark.table('fecha').na.drop()
df_fecha = (df_vuelo_fecha.groupBy('dia').count())
df_mas_vuelos = df_fecha.sort(f.col("count").desc()).limit(1)
df_menos_vuelos = df_fecha.sort(f.col("count")).limit(1)

print("Día con más vuelos")
print(df_mas_vuelos.show())
print("Día con menos vuelos")
print(df_menos_vuelos.show())

df_retrasos = spark.table("retrasos").na.drop()
df_retrasos = df_retrasos.groupBy('vuelo').sum('retraso') \
    .select(f.col("sum(retraso)").alias("dias_retraso"),f.col("vuelo").alias("vuelo_id"))

df_join = df_vuelo_fecha.join(df_retrasos, df_vuelo_fecha["vuelo"] == df_retrasos["vuelo_id"], 'inner')
df_join = df_join.select("vuelo", "dia","dias_retraso") 

df_top_dia_retrasos = (df_join.groupBy('dia').sum('dias_retraso')) \
    .sort(f.col("sum(dias_retraso)").desc()).limit(1)

print("Top dia retrasos")
print(df_top_dia_retrasos.show())