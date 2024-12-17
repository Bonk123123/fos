from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Загрузка CSV-файла
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Создаем RDD и применяем map
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
rdd2 = rdd.map(lambda x: x * 10)
print(rdd2.collect())
