from clickhouse_driver import Client

client = Client(host='localhost')

# Создаем таблицу events
client.execute('''
CREATE TABLE IF NOT EXISTS events (
    user_id UInt64,
    event_date Date,
    event_type String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY user_id;
''')
print("Таблица events создана успешно!")
