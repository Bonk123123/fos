from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Загрузка CSV-файла
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Фильтруем строки, где amount > 100
filtered = df.filter(df.amount > 100)
filtered.show()
