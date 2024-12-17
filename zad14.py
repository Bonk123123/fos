from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadCSV").getOrCreate()

# Загрузка CSV-файла
products_df = spark.read.csv("data_products.csv", header=True, inferSchema=True)
sales_df = spark.read.csv("data_sales.csv", header=True, inferSchema=True)

# Выполняем join двух DataFrame
joined = products_df.join(sales_df, "product_id", "inner")
joined.show()
