from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Загрузка CSV-файла
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Вывод первых 5 строк
df.show(5)
