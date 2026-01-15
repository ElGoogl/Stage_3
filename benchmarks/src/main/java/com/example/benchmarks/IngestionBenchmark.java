package com.example.benchmarks;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.example.crawler.Downloader;

/**
 * Measures ingestion throughput for workloads of different sizes.
 * Stage 3 update: Downloader.downloadAndSave returns Optional<Path>.
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
    private static final int MIN_ID = 1342;
    private static final int MAX_ID = 1400;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = MIN_ID;
        return nextBookId++;
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        Files.createDirectories(DATA_DIR);
    }

    /**
     * Optional cleanup so your benchmark directory doesn't grow forever.
     * Comment out if you want to keep files for inspection.
     */
    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        deleteRecursively(DATA_DIR);
    }

    // ---------- Benchmarks ----------

    /**
     * 1 book ingestion test
     */
    @Benchmark
    public void ingest1Book(Blackhole bh) {
        int id = getNextBookId();
        Optional<Path> saved = Downloader.downloadAndSave(id, DATA_DIR);
        bh.consume(saved.isPresent());
        saved.ifPresent(bh::consume);
    }

    /**
     * 5 books ingestion test
     */
    @Benchmark
    public void ingest5Books(Blackhole bh) {
        bh.consume(ingestN(5));
    }

    /**
     * 10 books ingestion test
     */
    @Benchmark
    public void ingest10Books(Blackhole bh) {
        bh.consume(ingestN(10));
    }

    /**
     * 50 books ingestion test
     */
    @Benchmark
    public void ingest50Books(Blackhole bh) {
        bh.consume(ingestN(50));
    }

    // ---------- Helpers ----------

    /**
     * Ingest N books and return number of successful saves.
     * Returning an int is handy for sanity-checking (and we consume it in Blackhole).
     */
    private int ingestN(int n) {
        int ok = 0;
        for (int i = 0; i < n; i++) {
            int id = getNextBookId();
            if (Downloader.downloadAndSave(id, DATA_DIR).isPresent()) {
                ok++;
            }
        }
        return ok;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;

        try (Stream<Path> s = Files.walk(root)) {
            s.sorted((a, b) -> b.compareTo(a)) // delete children before parents
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }
    }
}
