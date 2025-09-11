import os
import shutil
import clickhouse_connect

import sys
from pathlib import Path

from init_clickhouse import setup_project

project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from data_ingestion.config import (
    INPUT_DIR, PROCESSED_DIR,
    CLICKHOUSE_TABLE, CLICKHOUSE_HOST,
    CLICKHOUSE_PORT, CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD
)

RESULTS_DIR = Path(__file__).parent.parent / 'query_scenarios' / 'results'

def reset_files():
    print("🔁 Moving processed files back to input directory...")
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for filename in os.listdir(PROCESSED_DIR):
        src = os.path.join(PROCESSED_DIR, filename)
        dst = os.path.join(INPUT_DIR, filename)
        shutil.move(src, dst)
        print(f"→ Moved: {filename}")

def reset_clickhouse():
    print("🧨 Connecting to ClickHouse to drop table...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=False
    )

    try:
        client.command("DROP TABLE IF EXISTS mv_trip_stats_daily;")
        client.command("DROP TABLE IF EXISTS mv_trip_counts_daily;")
        client.command("DROP TABLE IF EXISTS mv_location_stats;")
        client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_TABLE};")
        print(f"✅ Table '{CLICKHOUSE_TABLE}' dropped.")
    except Exception as e:
        print(f"❌ Error dropping table: {e}")

def reset_scenario_results():
    if os.path.exists(RESULTS_DIR):
        print("🧹 Deleting scenarios' output results...")
        shutil.rmtree(RESULTS_DIR)
        print("✅ Results removed successfully!")


def main():
    reset_files()
    reset_clickhouse()
    reset_scenario_results()
    setup_project()
    print("🎉 Project reset complete.")

if __name__ == "__main__":
    main()

