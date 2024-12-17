from clickhouse_driver import Client

client = Client(host='localhost')

# Создаем внешнюю таблицу
client.execute('''
CREATE TABLE IF NOT EXISTS web_logs (
    ip String,
    url String,
    ts UInt64
)
ENGINE = File(TabSeparated, '/user/hive/warehouse/weblogs.tsv');
''')
print("Внешняя таблица web_logs создана успешно!")
