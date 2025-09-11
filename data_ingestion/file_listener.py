import os, time, glob, shutil, gc, traceback
from pathlib import Path

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype
import pyarrow.parquet as pq

from config import INPUT_DIR, PROCESSED_DIR, POLL_INTERVAL, CLICKHOUSE_TABLE
from preprocessor import preprocess_data

BATCH_ROWS = int(os.getenv("NYC_BATCH_ROWS", "50000"))

FAILED_DIR = os.path.join(Path(PROCESSED_DIR).parent, "failed_data")


def _ensure_dirs():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(FAILED_DIR, exist_ok=True)


def _describe_table_cols(client) -> list[str]:
    desc = client.query(f"DESCRIBE TABLE {CLICKHOUSE_TABLE}").result_rows
    return [row[0] for row in desc]


def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for c in df.columns:
        if is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c], errors="coerce")
            if getattr(df[c].dtype, "tz", None) is not None:
                try:
                    df[c] = df[c].dt.tz_localize(None)
                except (TypeError, AttributeError):
                    df[c] = df[c].dt.tz_convert("UTC").dt.tz_localize(None)

    df = df.where(pd.notnull(df), None)
    return df


def _insert_dataframe(client, df: pd.DataFrame, table_cols: list[str]):
    common = [c for c in table_cols if c in df.columns]
    if not common:
        raise RuntimeError("No overlapping columns between DataFrame and table schema")

    df2 = _sanitize_df(df[common])

    if hasattr(client, "insert_df"):
        client.insert_df(table=CLICKHOUSE_TABLE, df=df2)
        return

    col_names = list(df2.columns)
    col_values = [df2[c].tolist() for c in col_names] 
    client.insert(
        table=CLICKHOUSE_TABLE,
        data=col_values,
        column_names=col_names,
        columnar=True,         
    )

def watch_and_process(client):
    _ensure_dirs()
    print(f"✅ Connected to ClickHouse.")
    print(f"👀 Watching '{INPUT_DIR}' for new parquet files...")
    print(f"⚙️ Batch size: {BATCH_ROWS} rows  →  Target table: {CLICKHOUSE_TABLE}")

    try:
        table_cols = _describe_table_cols(client)
    except Exception:
        print("❌ Could not DESCRIBE TABLE. Full error follows:")
        traceback.print_exc()
        return

    while True:
        try:
            files = glob.glob(os.path.join(INPUT_DIR, "*.parquet"))
            if not files:
                time.sleep(POLL_INTERVAL)
                continue

            for filepath in files:
                fname = os.path.basename(filepath)
                print(f"📄 Found: {fname}")
                total_inserted = 0

                try:
                    pf = pq.ParquetFile(filepath)
                    for rec_batch in pf.iter_batches(batch_size=BATCH_ROWS):
                        if rec_batch.num_rows == 0:
                            continue

                        df_chunk = rec_batch.to_pandas(types_mapper=None)
                        df_clean = preprocess_data(df_chunk)

                        if df_clean.empty:
                            del df_chunk, df_clean, rec_batch
                            gc.collect()
                            continue

                        _insert_dataframe(client, df_clean, table_cols)
                        total_inserted += len(df_clean)

                        del df_chunk, df_clean, rec_batch
                        gc.collect()

                        if total_inserted < BATCH_ROWS or total_inserted % (BATCH_ROWS * 5) == 0:
                            print(f"   • inserted so far: {total_inserted:,} rows")

                except Exception:
                    print("💥 Ingestion failed for this file. Detailed traceback:")
                    traceback.print_exc()
                    try:
                        dst = os.path.join(FAILED_DIR, fname)
                        shutil.move(filepath, dst)
                        print(f"➡️  Moved to FAILED: {dst}")
                    except Exception:
                        print("⚠️  Could not move problematic file to FAILED.")
                else:
                    try:
                        dst = os.path.join(PROCESSED_DIR, fname)
                        shutil.move(filepath, dst)
                        print(f"✅ Processed ({total_inserted:,} rows) and moved: {fname}")
                    except Exception:
                        print("⚠️  Insert OK but move to PROCESSED failed. Keeping file in place.")

        except KeyboardInterrupt:
            print("🛑 Stopped by user.")
            break
        except Exception:
            print("❌ Outer loop error. Detailed traceback:")
            traceback.print_exc()
            time.sleep(POLL_INTERVAL)
