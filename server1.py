import socket
import threading
from clickhouse_connect import get_client
import clickhouse_connect
import time
import time, json, argparse, os, sys
from query_scenarios.scenario_runner import exec_one_query
from pathlib import Path

from query_scenarios.metrics_recorder import run_query_with_metrics, aggregate_metrics, save_scenario_figure, metrics_to_dict

# Server config
HOST = 'localhost'
PORT = 9001
BASE_DIR = Path(__file__).parent
any_client = False

# Global variables for client tracking
active_clients = 0
shutdown_flag = False 
results = []

def handle_client(conn, addr):
    
    try:
        # Receive priority (first line)
        priority_line = conn.recv(64).decode().strip()
        priority = int(priority_line) if priority_line.isdigit() else 1
        print(f"[>] Client {addr} connected with priority {priority}")

        if priority == 0:
            data = conn.recv(4096).decode()  # Receive and decode
            latency_list = json.loads(data)  # Convert back to list
            perform_maintenance_tasks(latency_list)
            return
        
        # Create a new ClickHouse client
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
        
        while True:
            try:
                # Receive query
                query = conn.recv(16384).decode().strip()

                if not query:
                    continue

                # Execute query
                result = exec_one_query(client, query)
                results.append(result)
                result_str = str(result)
                conn.sendall(result_str.encode())
                print(f"[✓] Processed query from {addr}")

            except (ConnectionResetError, BrokenPipeError):
                print(f"[-] Client {addr} disconnected abruptly")
                break
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                try:
                    conn.sendall(error_msg.encode())
                except (BrokenPipeError, ConnectionResetError):
                    print(f"[-] Client {addr} disconnected during error handling")
                    break

    except (ConnectionResetError, BrokenPipeError):
        print(f"[-] Client {addr} disconnected during initial handshake")
    except Exception as e:
        print(f"[!] Unexpected error with client {addr}: {e}")
    finally:
        try:
            conn.close()
            client.close()
        except:
            pass
        

def perform_maintenance_tasks(latency_list):
    metrics_list = [r["metrics"] for r in results]
    agg = aggregate_metrics(metrics_list)
    thr_list = [r["throughput"] for r in results]
    rows_list = [r["rows"] for r in results]

    avg_latency = sum(latency_list) / len(latency_list)
    avg_throughput = sum(thr_list) / len(thr_list)

    results_base = BASE_DIR / "results" / "normal"
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
        "queries": [{"latency_sec": l, "throughput_rows_per_sec": t, "rows": r} for l, t, r in zip(latency_list, thr_list, rows_list)],
        "aggregated_metrics": metrics_to_dict(agg),
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f"[scenario] saved figure: {plot_path}")
    print(f"[scenario] saved json:   {json_path}")
    print(f"✅ Scenario complete. Avg latency: {avg_latency:.4f}s, Avg throughput: {avg_throughput:.2f} rows/s")

def start_server():
    global shutdown_flag
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, PORT))
        server.listen()
        server.settimeout(1.0)  # Allow periodic checking of shutdown_flag

        print(f"[✓] Server listening on {HOST}:{PORT}")
        
        try:
            while not shutdown_flag:
                try:
                    conn, addr = server.accept()
                    thread = threading.Thread(target=handle_client, args=(conn, addr))
                    any_client = True
                    thread.daemon = True
                    thread.start()
                except socket.timeout:
                    # Timeout occurred, check if we need to shutdown
                    continue
                except Exception as e:
                    if not shutdown_flag:
                        print(f"[!] Error accepting connection: {e}")
                    
        except KeyboardInterrupt:
            print("\n[!] Server shutdown requested...")
        finally:
            shutdown_flag = True
            print("[✓] Server shutting down gracefully...")
            
            # Wait for all clients to disconnect
            if active_clients > 0:
                print(f"Waiting for {active_clients} clients to disconnect...")
            
            # Give clients time to finish
            time.sleep(2)
            print("[✓] Server shutdown complete")

if __name__ == "__main__":
    start_server()