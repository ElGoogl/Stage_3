package com.example.benchmarks;

import org.openjdk.jmh.annotations.*;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import com.example.crawler.Downloader;


/**
 * Measures Books downloaded per second
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class IngestionBenchmark {

    private static final Path DATA_DIR = Path.of("benchmark_datalake");
    private int nextBookId = 1342;

    /**
     * Download-Benchmark
     */
    @Benchmark
    public boolean testIngestion() {
        boolean ok = Downloader.downloadAndSave(nextBookId++, DATA_DIR);
        if (nextBookId > 1350) nextBookId = 1342;
        return ok;
    }
}
