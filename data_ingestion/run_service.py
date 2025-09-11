from clickhouse_client import get_clickhouse_client
from file_listener import watch_and_process

def main():
    print("🚀 Starting NYC Taxi Preprocessing Service...")
    client = get_clickhouse_client()
    watch_and_process(client)

if __name__ == "__main__":
    main()

