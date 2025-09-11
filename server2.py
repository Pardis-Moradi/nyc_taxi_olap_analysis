import socket
import threading
from queue import Queue
import queue, uuid, time, json, os, sys, hashlib
from pathlib import Path
from datetime import datetime, timezone

import clickhouse_connect

# ---- Metrics & plotting
from query_scenarios.metrics_recorder import (
    run_query_with_metrics,
    aggregate_metrics,
    save_scenario_figure,
    metrics_to_dict,
)
# برای ساخت مجدد dataclass از dict روی cache-hit
from query_scenarios.metrics_recorder import PhaseMetrics  # type: ignore
from query_scenarios.metrics_recorder import QueryMetrics  # type: ignore

# ---------------- Config ----------------
HOST = '0.0.0.0'
PORT = int(os.getenv("SERVER2_PORT", "9001"))
BASE_DIR = Path(__file__).parent
POOL_SIZE = int(os.getenv("POOL_SIZE", "10"))

# Cache settings
REDIS_ENABLED = os.getenv("REDIS_ENABLED", "1") == "1"
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL = int(os.getenv("REDIS_TTL", "300"))  # ثانیه

# اگر نتایج کش در سناریو هم تجمیع شوند
COUNT_CACHE_IN_SCENARIO = "1"

# -------------- Optional Redis --------------
redis_client = None
if REDIS_ENABLED:
    try:
        import redis  # type: ignore
        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        # تست سریع
        redis_client.ping()
        print(f"[cache] Redis connected at {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
    except Exception as e:
        print(f"[cache] Redis disabled (reason: {e}). Falling back to in-memory cache.")
        redis_client = None
else:
    print("[cache] Redis disabled by env. Using in-memory cache.")

# -------------- In-memory cache fallback --------------
# key -> {"value": <json str>, "exp": epoch_seconds}
_mem_cache = {}
_mem_lock = threading.Lock()
MEM_TTL = REDIS_TTL

def _mem_get(key: str):
    now = time.time()
    with _mem_lock:
        item = _mem_cache.get(key)
        if not item:
            return None
        if item["exp"] < now:
            _mem_cache.pop(key, None)
            return None
        return item["value"]

def _mem_setex(key: str, ttl: int, value: str):
    with _mem_lock:
        _mem_cache[key] = {"value": value, "exp": time.time() + ttl}

# ---------------- Task Queue & Results ----------------
task_queue = []
task_lock = threading.Lock()

results = []
results_lock = threading.Lock()

shutdown_flag = False

# ---------------- Helpers ----------------
def create_client_pool(pool_size: int) -> Queue:
    pool = Queue()
    for _ in range(pool_size):
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
        pool.put(client)
    return pool

def normalize_sql(sql: str) -> str:
    # حذف space اضافی/خط جدید/; انتهایی
    s = " ".join(sql.strip().split())
    if s.endswith(";"):
        s = s[:-1]
    return s

def cache_key_for_sql(sql: str) -> str:
    s = normalize_sql(sql)
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return f"ch:query:{h}"

def metrics_from_dict(d: dict) -> QueryMetrics:
    # d ساختار metrics_to_dict دارد
    def _pm(obj):
        return PhaseMetrics(pre=float(obj["pre"]), during=float(obj["during"]), post=float(obj["post"]))
    return QueryMetrics(
        cpu=_pm(d["cpu"]),
        memory_mb=_pm(d["memory_mb"]),
        threads=_pm(d["threads"]),
        fds=_pm(d["fds"]),
        net_kbps=_pm(d["net_kbps"]),
    )

def exec_query_with_metrics(db_client, sql: str) -> dict:
    """
    خروجی: {"metrics": QueryMetrics, "latency_s": float, "rows": int, "throughput": float, "source": "db|cache"}
    """
    # ---- Cache check
    print("query =", sql)
    key = cache_key_for_sql(sql)
    cached_json = None
    if redis_client is not None:
        try:
            cached_json = redis_client.get(key)
        except Exception:
            cached_json = None
    else:
        cached_json = _mem_get(key)


    if cached_json:
        try:
            payload = json.loads(cached_json)
            # اگر خواستی cache-hit هم در سناریو لحاظ شود
            if COUNT_CACHE_IN_SCENARIO:
                m = metrics_from_dict(payload["metrics"])
                with results_lock:
                    results.append({
                        "metrics": m,
                        "latency_s": float(payload.get("latency_s", 0.0)),
                        "rows": int(payload.get("rows", 0)),
                        "throughput": float(payload.get("throughput", 0.0)),
                        "source": "cache",
                    })
                payload["source"] = "cache"
                return payload
        except Exception:
            pass  # اگر خراب بود، می‌رویم سراغ اجرای واقعی

    # ---- Real execution + metrics
    result, m, latency_s = run_query_with_metrics(lambda: db_client.query(sql), post_sleep=0.25)
    rows = len(result.result_rows)
    thr = rows / latency_s if latency_s > 0 else 0.0

    payload = {
        "metrics": m,  # برای append به results
        "latency_s": latency_s,
        "rows": rows,
        "throughput": thr,
        "source": "db",
    }

    # ---- Save to scenario store (always for real DB runs)
    with results_lock:
        results.append(payload)

    # ---- Save to cache (store dict-ified metrics)
    cache_value = {
        "metrics": metrics_to_dict(m),
        "latency_s": latency_s,
        "rows": rows,
        "throughput": thr,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        value_str = json.dumps(cache_value, ensure_ascii=False)
        if redis_client is not None:
            redis_client.setex(key, REDIS_TTL, value_str)
        else:
            _mem_setex(key, MEM_TTL, value_str)
    except Exception as e:
        print(f"[cache] set failed: {e}")

    return payload

# ---------------- Dispatcher ----------------
def dispatcher(db_pool: Queue):
    print("[dispatcher] started")
    while True:
        db_client = db_pool.get()
        with task_lock:
            if not task_queue:
                db_pool.put(db_client)
                time.sleep(0.05)
                continue
            now = time.time()
            scored = [(priority * (now - ts), idx)
                      for idx, (_, _, priority, ts, _) in enumerate(task_queue)]
            _, best_idx = max(scored)
            conn, client_id, priority, ts, query = task_queue.pop(best_idx)

        try:
            result = exec_query_with_metrics(db_client, query)
            # پاسخ به کلاینت (JSON)
            try:
                conn.sendall(json.dumps({
                    "latency_s": result["latency_s"],
                    "rows": result["rows"],
                    "throughput": result["throughput"],
                    "source": result["source"],
                    # متریک‌ها اگر از DB است dataclass هستند؛ برای ارسال باید dict شوند
                    "metrics": (metrics_to_dict(result["metrics"]) if result["source"] == "db" else result["metrics"]),
                }).encode())
            except Exception:
                pass
            print(f"[✓] Query done for {client_id} (prio={priority}, source={result['source']})")

        except Exception as e:
            print(f"[!] Error executing query for {client_id}: {e}")

        finally:
            db_pool.put(db_client)

# ---------------- Client Handler ----------------
def handle_client(conn, addr):
    try:
        priority_data = conn.recv(64).decode().strip()
        priority = int(priority_data) if priority_data.isdigit() else 1
        client_id = str(uuid.uuid4())
        print(f"[>] Client {client_id} connected from {addr} with priority {priority}")

        # پیام نگه‌داری/خاتمه سناریو
        if priority == 0:
            data = conn.recv(8192).decode()
            latency_list = json.loads(data)
            perform_maintenance_tasks(latency_list)
            return


        while True:
            query_data = conn.recv(16384).decode().strip()
            if not query_data:
                continue
            ts = time.time()
            with task_lock:
                task_queue.append((conn, client_id, priority, ts, query_data))
            print(f"[+] Task queued from {client_id}")

    except Exception as e:
        print(f"[!] Client error ({addr}): {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        print(f"[-] Disconnected {addr}")

# ---------------- Scenario Aggregation ----------------
def perform_maintenance_tasks(latency_list: list[float]):
    with results_lock:
        snap = list(results)

    print(f"{len(results)}")
    if not snap:
        print("[!] No results collected yet; skipping aggregation.")
        return

    # some entries may come from cache with metrics as dict not dataclass
    metrics_list = []
    thr_list, rows_list = [], []
    for r in snap:
        m = r["metrics"]
        if isinstance(m, dict):
            m = metrics_from_dict(m)
        metrics_list.append(m)
        thr_list.append(float(r.get("throughput", 0.0)))
        rows_list.append(int(r.get("rows", 0)))

    agg = aggregate_metrics(metrics_list)
    avg_latency = sum(latency_list) / len(latency_list) if latency_list else 0.0
    avg_throughput = sum(thr_list) / len(thr_list) if thr_list else 0.0

    results_base = BASE_DIR / "results" / "optimized"
    RESULT_DIR = results_base
    PLOTS_DIR = RESULT_DIR / "plots"
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    plot_path = save_scenario_figure(
        agg,
        out_dir=str(PLOTS_DIR),
        kpi_avg_latency_s=avg_latency,
        kpi_avg_throughput_rps=avg_throughput
    )

    base = Path(plot_path).stem
    json_path = RESULT_DIR / f"{base}.json"
    out = {
        "avg_latency_sec": avg_latency,
        "avg_throughput_rows_per_sec": avg_throughput,
        "queries": [
            {"latency_sec": l, "throughput_rows_per_sec": t, "rows": r}
            for l, t, r in zip(latency_list, thr_list, rows_list)
        ],
        "aggregated_metrics": metrics_to_dict(agg),
        "count_cache_in_scenario": COUNT_CACHE_IN_SCENARIO,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[scenario] saved figure: {plot_path}")
    print(f"[scenario] saved json:   {json_path}")
    print(f"✅ Scenario complete. Avg latency: {avg_latency:.4f}s, Avg throughput: {avg_throughput:.2f} rows/s")

    with results_lock:
        results.clear()

# ---------------- Server Bootstrap ----------------
def start_server(host=HOST, port=PORT):
    global shutdown_flag

    db_pool = create_client_pool(pool_size=POOL_SIZE)
    for _ in range(POOL_SIZE):
        threading.Thread(target=dispatcher, args=(db_pool,), daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()

    print(f"[✓] Server listening on {host}:{port}")

    try:
        while not shutdown_flag:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\n[!] Server shutdown requested...")
    finally:
        shutdown_flag = True
        time.sleep(1.0)
        print("[✓] Server shutdown complete")

if __name__ == "__main__":
    start_server()
