#!/usr/bin/env python3
import json
import os
from pathlib import Path

try:
    import matplotlib.pyplot as plt
except ImportError as exc:
    raise SystemExit("matplotlib is required: pip install matplotlib") from exc


ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = ROOT / "benchmark_results"
PLOTS_DIR = RESULTS_DIR / "plots"


def latest_result(prefix: str) -> Path:
    candidates = sorted(RESULTS_DIR.glob(f"{prefix}-*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No results found for {prefix} in {RESULTS_DIR}")
    return candidates[-1]


def latest_run_id() -> str:
    candidates = sorted(RESULTS_DIR.glob("baseline-*.json"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No baseline results found in {RESULTS_DIR}")
    return candidates[-1].stem.replace("baseline-", "")


def result_for_run(prefix: str, run_id: str) -> Path:
    path = RESULTS_DIR / f"{prefix}-{run_id}.json"
    if path.exists():
        return path
    fallback = latest_result(prefix)
    print(f"[WARN] Missing {prefix} result for run {run_id}. Using {fallback.name} instead.")
    return fallback


def load_json(path: Path):
    with path.open() as fh:
        return json.load(fh)


def ensure_plots_dir():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def plot_scaling_throughput(scaling):
    sets = scaling.get("sets", [])
    labels = [s.get("name") for s in sets]
    docs_per_sec = [s["ingestion"].get("docs_per_second", 0) for s in sets]

    plt.figure(figsize=(6, 4))
    plt.bar(labels, docs_per_sec, color="#2f6f9f")
    plt.title("Ingestion Throughput vs. Scale")
    plt.ylabel("docs/s")
    plt.xlabel("Set")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "scaling_ingestion_throughput.png", dpi=160)
    plt.close()


def plot_scaling_search_latency(scaling):
    sets = scaling.get("sets", [])
    labels = [s.get("name") for s in sets]
    avg = [s["search"].get("avg_ms", 0) for s in sets]
    p95 = [s["search"].get("p95_ms", 0) for s in sets]

    plt.figure(figsize=(6, 4))
    plt.plot(labels, avg, marker="o", label="avg_ms")
    plt.plot(labels, p95, marker="o", label="p95_ms")
    plt.title("Search Latency vs. Scale")
    plt.ylabel("ms")
    plt.xlabel("Set")
    plt.ylim(bottom=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "scaling_search_latency.png", dpi=160)
    plt.close()


def plot_scaling_metadata_latency(scaling):
    sets = scaling.get("sets", [])
    labels = [s.get("name") for s in sets]
    avg = []
    p95 = []
    for s in sets:
        md = s.get("indexing", {}).get("metadata_lookup", {})
        avg.append(md.get("avg_ms", 0))
        p95.append(md.get("p95_ms", 0))

    plt.figure(figsize=(6, 4))
    plt.plot(labels, avg, marker="o", label="avg_ms")
    plt.plot(labels, p95, marker="o", label="p95_ms")
    plt.title("Metadata Lookup Latency vs. Scale")
    plt.ylabel("ms")
    plt.xlabel("Set")
    plt.ylim(bottom=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "scaling_metadata_latency.png", dpi=160)
    plt.close()


def plot_load_latency(load):
    search = load.get("search", {})
    labels = ["avg_ms", "p95_ms", "max_ms"]
    values = [search.get("avg_ms", 0), search.get("p95_ms", 0), search.get("max_ms", 0)]

    plt.figure(figsize=(6, 4))
    plt.bar(labels, values, color="#4c7a3d")
    plt.title("Load Test Search Latency")
    plt.ylabel("ms")
    plt.ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "load_search_latency.png", dpi=160)
    plt.close()


def plot_failure_recovery(failure):
    recovery = failure.get("recovery_time_seconds")
    value = recovery if recovery is not None else 0

    plt.figure(figsize=(4, 4))
    plt.bar(["recovery"], [value], color="#a34f4f")
    plt.title("Failure Recovery Time")
    plt.ylabel("seconds")
    plt.ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "failure_recovery_time.png", dpi=160)
    plt.close()


def main():
    ensure_plots_dir()

    run_id = os.environ.get("BENCH_RUN_ID") or latest_run_id()
    scaling = load_json(result_for_run("scaling", run_id))
    load = load_json(result_for_run("load", run_id))
    failure = load_json(result_for_run("failure", run_id))

    plot_scaling_throughput(scaling)
    plot_scaling_search_latency(scaling)
    plot_scaling_metadata_latency(scaling)
    plot_load_latency(load)
    plot_failure_recovery(failure)

    print(f"Plots written to {PLOTS_DIR}")


if __name__ == "__main__":
    main()
