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

# Добавляем индекс на столбец region
client.execute('''
ALTER TABLE sales ADD INDEX idx_region (region) TYPE set(0) GRANULARITY 1;
''')
print("Индекс добавлен успешно!")
