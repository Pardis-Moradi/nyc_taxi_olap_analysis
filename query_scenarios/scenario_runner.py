import time, json, argparse, os, sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import clickhouse_connect

BASE_DIR = Path(__file__).parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from metrics_recorder import (
    run_query_with_metrics,
    aggregate_metrics,
    save_scenario_figure,
    metrics_to_dict,
)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

QUERY_FILE = BASE_DIR / "queries.sql"
OPTIMIZED_QUERY_FILE = BASE_DIR.parent / "optimizations" / "optimized_queries.sql"

DEFAULT_NUM_CLIENTS = 5
DEFAULT_NUM_SCENARIOS = 1 

def load_queries(path: Path):
    text = path.read_text(encoding="utf-8")
    raw_queries = text.split("\n\n")
    queries = []
    for chunk in raw_queries:
        lines = [ln for ln in chunk.strip().splitlines() if ln.strip() and not ln.strip().startswith("--")]
        if lines:
            queries.append("\n".join(lines))
    return queries

def exec_one_query(client, query):
    print("query = ", query)
    if client == None:
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

    result, m, latency_s = run_query_with_metrics(lambda: client.query(query), post_sleep=0.25)
    rows = len(result.result_rows)
    throughput = rows / latency_s if latency_s > 0 else 0.0
    return {
        "metrics": m,
        "latency_s": latency_s,
        "rows": rows,
        "throughput": throughput,
    }

def run_scenario(queries: list[str], scenario_id: int, clients: int, results_base: Path):
    print(f"\n🚀 Running Scenario {scenario_id}")
    max_workers = min(clients, len(queries))
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(exec_one_query, None, q) for q in queries]
        for f in futures:
            results.append(f.result())

    metrics_list = [r["metrics"] for r in results]
    agg = aggregate_metrics(metrics_list)
    lat_list = [r["latency_s"] for r in results]
    thr_list = [r["throughput"] for r in results]
    rows_list = [r["rows"] for r in results]

    avg_latency = sum(lat_list) / len(lat_list)
    avg_throughput = sum(thr_list) / len(thr_list)

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
        "scenario_id": scenario_id,
        "avg_latency_sec": avg_latency,
        "avg_throughput_rows_per_sec": avg_throughput,
        "queries": [{"latency_sec": l, "throughput_rows_per_sec": t, "rows": r} for l, t, r in zip(lat_list, thr_list, rows_list)],
        "aggregated_metrics": metrics_to_dict(agg),
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[scenario] saved figure: {plot_path}")
    print(f"[scenario] saved json:   {json_path}")
    print(f"✅ Scenario {scenario_id} complete. Avg latency: {avg_latency:.4f}s, Avg throughput: {avg_throughput:.2f} rows/s")

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--optimized", action="store_true", help="Use optimized queries")
    p.add_argument("--clients", type=int, default=DEFAULT_NUM_CLIENTS, help="Number of concurrent clients")
    p.add_argument("--scenarios", type=int, default=DEFAULT_NUM_SCENARIOS, help="Number of scenarios (repetitions)")
    args = p.parse_args()

    if args.optimized:
        query_path = OPTIMIZED_QUERY_FILE
        results_base = BASE_DIR / "results" / "optimized"
    else:
        query_path = QUERY_FILE
        results_base = BASE_DIR / "results" / "normal"

    queries = load_queries(query_path)

    for i in range(1, args.scenarios + 1):
        run_scenario(queries, i, clients=args.clients, results_base=results_base)

if __name__ == "__main__":
    main()
