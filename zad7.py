from clickhouse_driver import Client

client = Client(host='localhost')

# Создаем таблицу sales
client.execute('''
CREATE TABLE IF NOT EXISTS sales (
    id UInt32,
    region String,
    amount Float32
) ENGINE = MergeTree()
PARTITION BY region
ORDER BY id;
''')

# Вставляем данные
client.execute('''
INSERT INTO sales (id, region, amount) VALUES (1, 'EU', 100.5), (2, 'ASIA', 200.0);
''')

# Выполняем запрос для агрегации данных
result = client.execute('''
SELECT region, SUM(amount) AS total_amount
FROM sales
GROUP BY region;
''')

# Вывод результата
for row in result:
    print(f"Region: {row[0]}, Total Amount: {row[1]}")
