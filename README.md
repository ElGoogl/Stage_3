# Stage 3 - Distributed Search Engine (Cluster Architecture)

Search engine prototype with distributed ingestion, indexing, and search. The system runs as a Docker-based cluster with a distributed datalake, Hazelcast in-memory inverted index, ActiveMQ broker, and an Nginx load balancer.

## Project Overview

Components:
- **Ingestion service**: crawls Project Gutenberg books, writes to a distributed datalake, and replicates files across peers.
- **Indexing service**: consumes ingestion events from ActiveMQ, builds a distributed inverted index in Hazelcast, and stores metadata.
- **Search service**: queries the Hazelcast index and returns ranked results.
- **ActiveMQ**: message broker for ingestion and indexing events.
- **Hazelcast**: distributed in-memory data grid for the inverted index and coordination.
- **Nginx**: load balancer for search requests.

## Run With Docker Compose

Build and start the cluster:
```bash
docker compose up -d --build
```

Check container status:
```bash
docker ps
```

Stop the cluster:
```bash
docker compose down
```

Optional smoke test:
```bash
./test_cluster.sh
```

## Service Endpoints

Load balancer (Nginx):
- `http://localhost:8000/search?q=adventure`
- `http://localhost:8000/status`

Ingestion:
- `POST http://localhost:7001/ingest/{book_id}`
- `GET  http://localhost:7001/ingest/status/{book_id}`
- `GET  http://localhost:7001/ingest/list`

Indexing:
- `POST http://localhost:7101/index` with JSON body `{ "lakePath": "YYYYMMDD/HH/BOOK_ID.json" }`
- `GET  http://localhost:7101/health`

ActiveMQ web console:
- `http://localhost:8161` (admin/admin)

Example ingestion request:
```bash
curl -X POST "http://localhost:7001/ingest/1342"
```

Example search request:
```bash
curl "http://localhost:8000/search?q=love%20story&limit=10"
```

## Benchmarking (System-Level)

The system-level benchmark harness is in `benchmarks/` and produces JSON and CSV outputs in `benchmark_results/`.

Build the benchmarks:
```bash
mvn -pl benchmarks -am package
```

Run the scenarios:
```bash
java -jar benchmarks/target/benchmarks.jar baseline
java -jar benchmarks/target/benchmarks.jar scaling
java -jar benchmarks/target/benchmarks.jar load
java -jar benchmarks/target/benchmarks.jar failure
```

Optional clean reset between runs:
```bash
benchmarks/run_with_reset.sh scaling
benchmarks/run_with_reset.sh load
benchmarks/run_with_reset.sh failure
```

Outputs:
- `benchmark_results/*.json` summary metrics
- `benchmark_results/*-latency.csv` raw latency samples

Full details: `benchmarks/BENCHMARKING.md`.

## Benchmarking (Search Microbenchmarks)

Microbenchmarks for the search service (JMH) are in `search_service/`. See `search_service/BENCHMARKING.md`.

## Configuration Notes

Cluster configuration lives in:
- `docker-compose.yml` (services and scaling)
- `hazelcast.xml` (Hazelcast cluster and backup settings)
- `nginx.conf` (load balancing)
- `config/` (shared Hazelcast client config and docs)

## Demo Video

YouTube link (required):
- TODO: add the unlisted link here

