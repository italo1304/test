from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, IntegerType

# warehouse_location points to the default location for managed databases and tables
warehouse_location = '/opt/hadoop/hive/'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType() \
    .add("vuelo", IntegerType(), False) \
    .add("dia", IntegerType(), False) \

df = spark.read.format("csv").option("delimiter","_") \
    .option("header", False).schema(schema) \
    .load("hdfs://localhost:8020/user/vagrant/ejercicio/vuelos/fecha.dat")

print(df.show())
# spark.sql("CREATE TABLE IF NOT EXISTS vuelo (vuelo INT, dia int) USING hive")
#spark.sql("DROP TABLE vuelo")

#print(spark.sql("SHOW TABLES").show())


