import os
import time
import psutil
import threading
from dataclasses import dataclass, asdict
from typing import Dict, Tuple, Callable, Any, List
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import datetime

@dataclass
class PhaseMetrics:
    pre: float
    during: float
    post: float

@dataclass
class QueryMetrics:
    cpu: PhaseMetrics
    memory_mb: PhaseMetrics
    threads: PhaseMetrics
    fds: PhaseMetrics
    net_kbps: PhaseMetrics     
def _proc() -> psutil.Process:
    return psutil.Process()

def _snap_proc():
    p = _proc()
    cpu = psutil.cpu_percent(interval=0.1) 
    mem = p.memory_info().rss / (1024 ** 2)
    threads = p.num_threads()
    num_fds = p.num_fds() if hasattr(p, "num_fds") else 0
    return cpu, mem, threads, num_fds

def _avg(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0

def _net_bytes_total() -> int:
    io = psutil.net_io_counters(pernic=False)
    if not io:
        return 0
    return int(io.bytes_sent + io.bytes_recv)

def _rate_over(window_sec: float, getter: Callable[[], int]) -> float:
    t0 = time.perf_counter()
    v0 = getter()
    time.sleep(window_sec)
    v1 = getter()
    dt = max(time.perf_counter() - t0, 1e-6)
    return max(0.0, (v1 - v0) / dt)

class DuringSampler:
    def __init__(self, interval_sec: float = 0.1):
        self.interval = interval_sec
        self._stop = threading.Event()
        self._cpu: List[float] = []
        self._mem: List[float] = []
        self._thr: List[float] = []
        self._fds: List[float] = []
        self._net: List[int] = [] 
        self._t = None
        self._len = 0

    def start(self):
        self._t = threading.Thread(target=self._run, daemon=True)
        self._t.start()

    def _run(self):
        psutil.cpu_percent(interval=None)  # warm-up
        while not self._stop.is_set():
            cpu, mem, thr, fds = _snap_proc()
            self._cpu.append(cpu)
            self._mem.append(mem)
            self._thr.append(thr)
            self._fds.append(fds)
            self._net.append(_net_bytes_total())
            self._len += 1
            time.sleep(self.interval)

    def stop(self):
        self._stop.set()
        if self._t is not None:
            self._t.join(timeout=1.0)

    def stats(self) -> Dict[str, float]:
        net_kbps = 0.0
        if self._len >= 2:
            span = max((self._len - 1) * self.interval, 1e-6)
            delta_bytes = max(0.0, float(self._net[-1] - self._net[0]))
            net_kbps = (delta_bytes / span) / 1024.0

        return dict(
            cpu=_avg(self._cpu),
            mem=_avg(self._mem),
            thr=_avg(self._thr),
            fds=_avg(self._fds),
            net_kbps=net_kbps,
        )

def run_query_with_metrics(run_query_fn: Callable[[], Any], post_sleep: float = 0.25):
    cpu_pre, mem_pre, thr_pre, fds_pre = _snap_proc()
    pre_net_rate_kbps = (_rate_over(0.2, _net_bytes_total)) / 1024.0  # bytes/s → KB/s

    sampler = DuringSampler(interval_sec=0.1)
    sampler.start()
    t0 = time.perf_counter()
    result = run_query_fn()
    latency = time.perf_counter() - t0
    sampler.stop()
    during_stats = sampler.stats()

    time.sleep(post_sleep)
    cpu_post, mem_post, thr_post, fds_post = _snap_proc()
    post_net_rate_kbps = (_rate_over(0.2, _net_bytes_total)) / 1024.0

    m = QueryMetrics(
        cpu=PhaseMetrics(cpu_pre, during_stats["cpu"], cpu_post),
        memory_mb=PhaseMetrics(mem_pre, during_stats["mem"], mem_post),
        threads=PhaseMetrics(thr_pre, during_stats["thr"], thr_post),
        fds=PhaseMetrics(fds_pre, during_stats["fds"], fds_post),
        net_kbps=PhaseMetrics(pre_net_rate_kbps, during_stats["net_kbps"], post_net_rate_kbps),
    )
    return result, m, latency

def _agg_phase(values: List[PhaseMetrics]) -> PhaseMetrics:
    return PhaseMetrics(
        pre=_avg([v.pre for v in values]),
        during=_avg([v.during for v in values]),
        post=_avg([v.post for v in values]),
    )

def aggregate_metrics(metrics_list: List[QueryMetrics]) -> QueryMetrics:
    return QueryMetrics(
        cpu=_agg_phase([m.cpu for m in metrics_list]),
        memory_mb=_agg_phase([m.memory_mb for m in metrics_list]),
        threads=_agg_phase([m.threads for m in metrics_list]),
        fds=_agg_phase([m.fds for m in metrics_list]),
        net_kbps=_agg_phase([m.net_kbps for m in metrics_list]),
    )

SAVE_LOCK = threading.Lock()

def _beautify(ax, title, ylabel=None):
    ax.set_title(title, pad=10, fontsize=12)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", alpha=0.25)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

def _bar(ax, labels, values, fmt=None):
    bars = ax.bar(labels, values)
    try:
        ax.bar_label(bars, fmt=fmt if fmt else "%.2f", padding=3)
    except Exception:
        pass
    return bars

def save_scenario_figure(
    agg: QueryMetrics,
    out_dir: str,
    kpi_avg_latency_s: float | None = None,
    kpi_avg_throughput_rps: float | None = None,
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    out_path = os.path.join(out_dir, f"{ts}.png")

    fig = plt.figure(figsize=(16, 8), constrained_layout=True)
    gs = fig.add_gridspec(2, 3)
    ax_mem  = fig.add_subplot(gs[0, 0])
    ax_cpu  = fig.add_subplot(gs[0, 1])
    ax_thr  = fig.add_subplot(gs[0, 2])
    ax_fds  = fig.add_subplot(gs[1, 0])
    ax_net  = fig.add_subplot(gs[1, 1])

    x = ["Pre", "During", "Post"]

    _bar(ax_mem, x, [agg.memory_mb.pre, agg.memory_mb.during, agg.memory_mb.post], fmt="%.0f")
    _beautify(ax_mem, "Memory Usage (avg)", "MB")

    _bar(ax_cpu, x, [agg.cpu.pre, agg.cpu.during, agg.cpu.post], fmt="%.0f")
    _beautify(ax_cpu, "CPU Usage (avg)", "%")

    _bar(ax_thr, x, [agg.threads.pre, agg.threads.during, agg.threads.post], fmt="%.0f")
    _beautify(ax_thr, "Threads (avg)", "count")

    _bar(ax_fds, x, [agg.fds.pre, agg.fds.during, agg.fds.post], fmt="%.0f")
    _beautify(ax_fds, "Open FDs (avg)", "count")

    _bar(ax_net, x, [agg.net_kbps.pre, agg.net_kbps.during, agg.net_kbps.post], fmt="%.1f")
    _beautify(ax_net, "Network Rate (avg)", "KB/s")

    fig.suptitle(f"Scenario Averages over queries", fontsize=14, y=0.98)
    if kpi_avg_latency_s is not None:
        fig.text(0.01, 0.995, f"Avg Latency: {kpi_avg_latency_s*1000:.2f} ms", va="top", fontsize=11)
    if kpi_avg_throughput_rps is not None:
        fig.text(0.26, 0.995, f"Avg Throughput: {kpi_avg_throughput_rps:.2f} rows/s", va="top", fontsize=11)

    with SAVE_LOCK:
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path

def metrics_to_dict(m: QueryMetrics) -> Dict:
    return {
        "cpu": asdict(m.cpu),
        "memory_mb": asdict(m.memory_mb),
        "threads": asdict(m.threads),
        "fds": asdict(m.fds),
        "net_kbps": asdict(m.net_kbps),
    }
