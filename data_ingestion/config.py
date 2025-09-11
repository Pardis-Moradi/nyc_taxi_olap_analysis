import os
from pathlib import Path

INPUT_DIR = Path(__file__).parent / "input_data"
PROCESSED_DIR = Path(__file__).parent / "processed_data"
POLL_INTERVAL = 10  # seconds

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_TABLE = 'ny_taxi_trips'

