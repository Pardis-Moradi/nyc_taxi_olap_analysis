from clickhouse_connect import get_client
from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_TABLE

def get_clickhouse_client():
    client = get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=False
    )
    client.query('SELECT 1')
    print("✅ Connected to ClickHouse.")
    return client