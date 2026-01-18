# Stage 3 Benchmark Harness

This module provides a system-level benchmark harness aligned with the Stage 3 benchmarking methodology. It drives the **running services over HTTP** and produces JSON/CSV outputs for plotting.

## Scenarios

The harness supports four scenarios:

- **baseline**: single-node ingestion/indexing/search metrics
- **scaling**: multiple configurations to compare throughput vs. scale
- **load**: concurrent search load with latency distribution
- **failure**: execute a failure command and measure recovery time

## Configuration

Default settings live in `src/main/resources/benchmark-scenarios.json`. You can override with a custom file:

```bash
java -jar target/benchmarks.jar baseline --config /path/to/benchmark-scenarios.json
```

Important fields:

- `ingestionUrl`, `indexingUrl`, `searchUrl`: base URLs for services
- `bookIds`: list of Project Gutenberg IDs to ingest
- `queries`: search query strings
- `failure.failureCommand`: command to simulate node failure (e.g., `docker stop search2`)
- `dockerStats`: optional container list to capture CPU/memory

## Running

```bash
mvn -pl benchmarks -am package
java -jar benchmarks/target/benchmarks.jar baseline
java -jar benchmarks/target/benchmarks.jar scaling
java -jar benchmarks/target/benchmarks.jar load
java -jar benchmarks/target/benchmarks.jar failure
```

## Reset + Run

If you need a clean Docker state between benchmark runs, use:

```bash
benchmarks/run_with_reset.sh baseline
```

This stops the cluster, clears `data_repository` (keeps `.gitignore`/`.gitkeep`), starts the cluster again, and then runs the scenario.

You can also run the reset from the benchmark runner:

```bash
java -jar benchmarks/target/benchmarks.jar baseline --reset
```

On Windows, `--reset` uses `benchmarks/reset_cluster.ps1`. On macOS/Linux it uses `benchmarks/reset_cluster.sh`.

## Output

Results are stored in `benchmark_results/`:

- `*.json`: scenario summary with throughput, latency, and recovery stats
- `*-latency.csv`: raw latency samples for plotting percentiles

These outputs can be used to create the required tables and charts (throughput vs. nodes, latency distributions, CPU/memory snapshots, recovery times).
