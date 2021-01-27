from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType

# warehouse_location points to the default location for managed databases and tables
warehouse_location = '/user/hive/warehouse/'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("""CREATE DATABASE IF NOT EXISTS air""")
spark.sql("""USE air""")
"""
schema = StructType() \
    .add("vuelo", IntegerType(), False) \
    .add("dia", IntegerType(), False)

df = spark.read.format("csv").option("delimiter","_") \
    .option("header", False).schema(schema) \
    .load("ejercicio/fecha/fecha.dat")

df.write.mode("overwrite").saveAsTable("vuelo_fecha")

print(spark.sql("SHOW TABLES").show())

df_load = spark.sql('SELECT * FROM vuelo_fecha')
print(df_load.show())
"""

schema = StructType() \
    .add("cod_pais", StringType(), False) \
    .add("pais", StringType(), False)

df = spark.read.format("csv").option("encoding", "UTF-8") \
    .option("delimiter",";") \
    .option("header", False).schema(schema) \
    .load("ejercicio/paises/paises.ada")

df.write.mode("overwrite").saveAsTable("pais")

print(spark.sql("SHOW TABLES").show())

df_load = spark.sql('SELECT * FROM pais')
print(df_load.show())

# spark.sql("CREATE TABLE IF NOT EXISTS vuelo (vuelo INT, dia int) USING hive")
#spark.sql("DROP TABLE vuelo")

#print(spark.sql("SHOW TABLES").show())

#####
# spark.conf.set("spark.sql.crossJoin.enabled", "true")
df_mas_vuelos = df_fecha.sort(f.col("count").desc()).limit(1)
df_menos_vuelos = df_fecha.sort(f.col("count")).limit(1)

df_mas_vuelos = df_mas_vuelos.select(f.col("*"),f.lit(1).alias("key"))
df_menos_vuelos = df_menos_vuelos.select(f.col("*"),f.lit(1).alias("key"))

df_m_m = df_mas_vuelos.join(df_menos_vuelos, "key", 'inner')
df_m_m = df_m_m.select("dia[0]","count[0]")