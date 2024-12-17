from clickhouse_driver import Client

client = Client(host='localhost')

# Создаем таблицу user_info
client.execute('''
CREATE TABLE IF NOT EXISTS user_info (
    ip String,
    user_id UInt32
) ENGINE = MergeTree()
ORDER BY user_id;
''')

# Вставляем данные
client.execute('''
INSERT INTO user_info (ip, user_id) VALUES
('192.168.1.1', 1),
('192.168.1.2', 2);
''')

# Выполняем JOIN
result = client.execute('''
SELECT
    ui.user_id,
    COUNT(*) AS total_visits
FROM web_logs wl
JOIN user_info ui
ON wl.ip = ui.ip
GROUP BY ui.user_id;
''')

# Вывод результата
for row in result:
    print(f"User ID: {row[0]}, Total Visits: {row[1]}")
