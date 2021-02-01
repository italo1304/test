from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.window as w

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .enableHiveSupport() \
    .getOrCreate()

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.window as w

df_vuelos = spark.table("vuelos").na.drop()

df_salidas = (df_vuelos.groupBy('origen').count()).sort(f.col("count").desc()).limit(1)
df_llegadas = (df_vuelos.groupBy('destino').count()).sort(f.col("count").desc()).limit(1)

df_pais = spark.table("paises").na.drop()

df_salidas = df_salidas.join(df_pais, df_pais["cod_pais"] == df_salidas["origen"], 'inner')
df_salidas = df_salidas.select("pais", f.col("count").alias("top_salidas"))

df_llegadas = df_llegadas.join(df_pais, df_pais["cod_pais"] == df_llegadas["destino"], 'inner')
df_llegadas = df_llegadas.select("pais", f.col("count").alias("top_destino"))

# 4a
print("País top salida vuelos")
print(df_salidas.show())
# 4b
print("País top llegada vuelos")
print(df_llegadas.show())

df_vuelo_fecha = spark.table('fecha').na.drop()
df_fecha = (df_vuelo_fecha.groupBy('dia').count())
df_mas_vuelos = df_fecha.sort(f.col("count").desc()).limit(1)
df_menos_vuelos = df_fecha.sort(f.col("count")).limit(1)

# 4c
print("Día con más vuelos")
print(df_mas_vuelos.show())
# 4d
print("Día con menos vuelos")
print(df_menos_vuelos.show())

df_retrasos = spark.table("retrasos").na.drop()
df_retrasos = df_retrasos.groupBy('vuelo').sum('retraso') \
    .select(f.col("sum(retraso)").alias("dias_retraso"),f.col("vuelo").alias("vuelo_id"))

df_join = df_vuelo_fecha.join(df_retrasos, df_vuelo_fecha["vuelo"] == df_retrasos["vuelo_id"], 'inner')
df_join = df_join.select("vuelo", "dia","dias_retraso") 

df_top_dia_retrasos = (df_join.groupBy('dia').sum('dias_retraso')) \
    .sort(f.col("sum(dias_retraso)").desc()).limit(1)

df_top_dia_m_retrasos = (df_join.groupBy('dia').sum('dias_retraso')) \
    .sort(f.col("sum(dias_retraso)")).limit(1)

print("Top dia retrasos")
print(df_top_dia_retrasos.show())

print("Top dia menos retrasos")
print(df_top_dia_m_retrasos.show())

df_vuelos_retraso = df_vuelos.join(df_retrasos, df_vuelos["vuelo"] == df_retrasos["vuelo_id"], 'inner')
df_vuelos_retraso = df_vuelos_retraso.select("vuelo", "origen", "destino","dias_retraso")

df_vuelos_retraso = df_vuelos_retraso.join(df_pais, df_pais["cod_pais"] == df_vuelos_retraso["origen"], 'inner')
df_vuelos_retraso = df_vuelos_retraso.select("vuelo", f.col("pais").alias("origen"), "destino","dias_retraso")

df_vuelos_retraso = df_vuelos_retraso.join(df_pais, df_pais["cod_pais"] == df_vuelos_retraso["destino"], 'inner')
df_vuelos_retraso = df_vuelos_retraso.select("vuelo", "origen", f.col("pais").alias("destino"),"dias_retraso")

print("Retraso Vuelos")
print(df_vuelos_retraso.show())

#df_top_dia_retrasos.write.mode("overwrite").saveAsTable("top_dia_retrasos")
df_retraso_acumulado = df_join.join(df_vuelos, "vuelo", 'inner')
df_retraso_acumulado = df_retraso_acumulado.groupBy('origen', 'dia').sum('dias_retraso')
df_retraso_acumulado = df_retraso_acumulado.select("origen","dia",f.col("sum(dias_retraso)").alias("retraso"))
window = w.Window.partitionBy(f.col("origen")).orderBy(f.col("retraso"))
df_retraso_acumulado = df_retraso_acumulado.withColumn("retraso_acumulado", f.sum("retraso").over(window))

df2 = df_retraso_acumulado.repartition(4)

##testing
"""
pais_vip = ["Peru", "España", "Mexico"]
udf_pais_vip = f.udf(lambda x : "VIP" if x in pais_vip else "NO VIP")

df = df_salidas.select(f.col("pais"),f.col("top_salidas"), udf_pais_vip(f.col("pais")).alias("vip"))

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]
df = spark.createDataFrame(data=data,schema=columns)
"""