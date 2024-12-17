from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Загрузка CSV-файла
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Создаем временное представление
df.createOrReplaceTempView("transactions_view")

# Выполняем SQL-запрос
result = spark.sql("SELECT product, COUNT(*) FROM transactions_view GROUP BY product")
result.show()
