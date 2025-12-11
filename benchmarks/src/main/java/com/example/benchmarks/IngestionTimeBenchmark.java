package com.example.benchmarks;

import com.example.crawler.Downloader;
import org.openjdk.jmh.annotations.*;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark measuring ingestion time for different batch sizes.
 * Each benchmark method measures the time to ingest a specific number of books.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class IngestionTimeBenchmark {

    private static final Path DATA_DIR = Path.of("benchmark_datalake");
    private int nextBookId = 1342;
    private static final int MAX_ID = 1400;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = 1342;
        return nextBookId++;
    }

    /** Measure time for ingesting 1 book */
    @Benchmark
    public void ingest1Book() {
        Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
    }

    /** Measure time for ingesting 5 books */
    @Benchmark
    public void ingest5Books() {
        for (int i = 0; i < 5; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }

    /** Measure time for ingesting 10 books */
    @Benchmark
    public void ingest10Books() {
        for (int i = 0; i < 10; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }

    /** Measure time for ingesting 50 books */
    @Benchmark
    public void ingest50Books() {
        for (int i = 0; i < 50; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }
    }
}
