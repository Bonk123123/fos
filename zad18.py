from clickhouse_driver import Client

client = Client(host='localhost')

# Выполняем группировку и подсчитываем количество строк
result = client.execute('''
SELECT url, COUNT(*) AS visit_count
FROM web_logs
GROUP BY url;
''')

# Вывод результата
for row in result:
    print(f"URL: {row[0]}, Visit Count: {row[1]}")
