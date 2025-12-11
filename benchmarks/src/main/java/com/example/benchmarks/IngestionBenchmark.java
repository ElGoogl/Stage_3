package com.example.benchmarks;

import org.openjdk.jmh.annotations.*;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import com.example.crawler.Downloader;

/**
 * Measures ingestion throughput for workloads of different sizes
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class IngestionBenchmark {

    private static final Path DATA_DIR = Path.of("benchmark_datalake");
    private int nextBookId = 1342;

    private static final int MAX_ID = 1400;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = 1342;
        return nextBookId++;
    }

    /**
     * 1 book ingestion test
     */
    @Benchmark
    public boolean ingest1Book() {
        int id = getNextBookId();
        return Downloader.downloadAndSave(id, DATA_DIR);
    }

    /**
     * 5 books ingestion test
     */
    @Benchmark
    public void ingest5Books() {
        for (int i = 0; i < 5; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }

    /**
     * 10 books ingestion test
     */
    @Benchmark
    public void ingest10Books() {
        for (int i = 0; i < 10; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }

    /**
     * 50 books ingestion test
     */
    @Benchmark
    public void ingest50Books() {
        for (int i = 0; i < 50; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }
}
