#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, glob, argparse
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use("Agg")  # مناسب سرور/WSL
import matplotlib.pyplot as plt

METRIC_KEYS = ["cpu", "memory_mb", "threads", "fds", "net_kbps"]
PHASES = ["pre", "during", "post"]

def mean(lst):
    return (sum(lst) / len(lst)) if lst else 0.0

def load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def collect_from_files(files: list[Path]) -> tuple[dict, float, float, int]:
    n = len(files)
    if n == 0:
        raise RuntimeError("هیچ فایل JSONی پیدا نشد.")

    sums = {k: {p: 0.0 for p in PHASES} for k in METRIC_KEYS}
    latencies = []
    throughputs = []

    for p in files:
        data = load_json(p)
        agg = data.get("aggregated_metrics") or {}
        for mk in METRIC_KEYS:
            m = agg.get(mk) or {}
            for ph in PHASES:
                sums[mk][ph] += float(m.get(ph, 0.0))
        if "avg_latency_sec" in data:
            latencies.append(float(data["avg_latency_sec"]))
        if "avg_throughput_rows_per_sec" in data:
            throughputs.append(float(data["avg_throughput_rows_per_sec"]))

    avg_metrics = {mk: {ph: (sums[mk][ph] / n) for ph in PHASES} for mk in METRIC_KEYS}
    avg_latency = mean(latencies)
    avg_throughput = mean(throughputs)
    return avg_metrics, avg_latency, avg_throughput, n

def beautify(ax, title, ylabel=None):
    ax.set_title(title, pad=10, fontsize=12)
    if ylabel:
        ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", alpha=0.25)
    for spine in ["top", "right"]:
        ax.spines[spine].set_visible(False)

def bar3(ax, values, fmt="%.2f"):
    labels = ["Pre", "During", "Post"]
    bars = ax.bar(labels, values)
    try:
        ax.bar_label(bars, fmt=fmt, padding=3)
    except Exception:
        pass

def make_figure(avg_metrics: dict, avg_latency: float, avg_throughput: float, n_files: int, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"combined_{n_files}files_{ts}.png"

    fig = plt.figure(figsize=(16, 8), constrained_layout=True)
    gs = fig.add_gridspec(2, 3)
    ax_mem  = fig.add_subplot(gs[0, 0])
    ax_cpu  = fig.add_subplot(gs[0, 1])
    ax_thr  = fig.add_subplot(gs[0, 2])
    ax_fds  = fig.add_subplot(gs[1, 0])
    ax_net  = fig.add_subplot(gs[1, 1])

    bar3(ax_mem,  [avg_metrics["memory_mb"]["pre"], avg_metrics["memory_mb"]["during"], avg_metrics["memory_mb"]["post"]], fmt="%.0f")
    beautify(ax_mem, "Memory Usage (avg)", "MB")

    bar3(ax_cpu,  [avg_metrics["cpu"]["pre"], avg_metrics["cpu"]["during"], avg_metrics["cpu"]["post"]], fmt="%.0f")
    beautify(ax_cpu, "CPU Usage (avg)", "%")

    bar3(ax_thr,  [avg_metrics["threads"]["pre"], avg_metrics["threads"]["during"], avg_metrics["threads"]["post"]], fmt="%.0f")
    beautify(ax_thr, "Threads (avg)", "count")

    bar3(ax_fds,  [avg_metrics["fds"]["pre"], avg_metrics["fds"]["during"], avg_metrics["fds"]["post"]], fmt="%.0f")
    beautify(ax_fds, "Open FDs (avg)", "count")

    bar3(ax_net,  [avg_metrics["net_kbps"]["pre"], avg_metrics["net_kbps"]["during"], avg_metrics["net_kbps"]["post"]], fmt="%.1f")
    beautify(ax_net, "Network Rate (avg)", "KB/s")

    fig.suptitle(f"Combined Averages over {n_files} scenarios", fontsize=14, y=0.98)
    fig.text(0.01, 0.995, f"Avg Latency: {avg_latency*1000:.2f} ms", va="top", fontsize=11)
    fig.text(0.26, 0.995, f"Avg Throughput: {avg_throughput:.2f} rows/s", va="top", fontsize=11)

    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path

def save_json(avg_metrics: dict, avg_latency: float, avg_throughput: float, n_files: int, out_dir: Path, img_path: Path) -> Path:
    payload = {
        "files_combined": n_files,
        "avg_latency_sec": avg_latency,
        "avg_throughput_rows_per_sec": avg_throughput,
        "aggregated_metrics": avg_metrics,
        "figure_path": str(img_path),
    }
    path = out_dir / (img_path.stem + ".json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path

def main():
    ap = argparse.ArgumentParser(description="Combine scenario JSONs and plot averaged 3-phase metrics.")
    ap.add_argument("inputs", nargs="+",
                    help="لیست مسیرها یا الگوهای glob برای JSONها (مثلاً results/normal/*.json)")
    ap.add_argument("-o", "--outdir", default="query_scenarios/results/combined",
                    help="پوشهٔ خروجی برای تصویر و JSON میانگین")
    args = ap.parse_args()

    # گسترش glob
    files: list[Path] = []
    for pat in args.inputs:
        matches = [Path(p) for p in glob.glob(pat)]
        files.extend([m for m in matches if m.suffix.lower() == ".json"])
    files = sorted(set(files))

    avg_metrics, avg_latency, avg_throughput, n = collect_from_files(files)

    out_dir = Path(args.outdir)
    img_path = make_figure(avg_metrics, avg_latency, avg_throughput, n, out_dir)
    json_path = save_json(avg_metrics, avg_latency, avg_throughput, n, out_dir, img_path)

    print(f"🖼  saved figure: {img_path}")
    print(f"🧾 saved json:   {json_path}")

if __name__ == "__main__":
    main()
