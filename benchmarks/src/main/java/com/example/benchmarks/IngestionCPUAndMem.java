package com.example.benchmarks;

import com.example.crawler.Downloader;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark measuring ingestion time, CPU and memory usage.
 * Stage 3 update: Downloader.downloadAndSave returns Optional<Path>.
 *
 * Notes:
 * - Avoid printing inside benchmarks (System.out) because it ruins measurements.
 * - We capture "before/after" metrics into fields that you can read after the run.
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Benchmark)
public class IngestionCPUAndMem {

    private static final Path DATA_DIR = Path.of("benchmark_datalake");

    private static final OperatingSystemMXBean osBean =
            (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    private int nextBookId = 1342;
    private static final int MIN_ID = 1342;
    private static final int MAX_ID = 1400;

    // Last recorded metrics (so you can inspect after benchmark)
    private volatile double lastCpuBefore;
    private volatile double lastCpuAfter;
    private volatile double lastUsedMbBefore;
    private volatile double lastUsedMbAfter;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = MIN_ID;
        return nextBookId++;
    }

    // ---------- Benchmarks ----------

    /** Average time per single ingestion */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void ingestOneBookAvgTime(Blackhole bh) {
        recordSystemUsageBefore();

        Optional<Path> saved = Downloader.downloadAndSave(getNextBookId(), DATA_DIR);

        recordSystemUsageAfter();

        // Consume outcome to prevent dead-code elimination
        bh.consume(saved.isPresent());
        saved.ifPresent(bh::consume);
    }

    /**
     * Batch ingestion of 10 books.
     * Measures the whole batch as a single shot.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void ingestTenBooksCpuMem(Blackhole bh) {
        recordSystemUsageBefore();

        int ok = 0;
        for (int i = 0; i < 10; i++) {
            if (Downloader.downloadAndSave(getNextBookId(), DATA_DIR).isPresent()) {
                ok++;
            }
        }

        recordSystemUsageAfter();
        bh.consume(ok);
    }

    /** Throughput test (one ingestion operation per invocation) */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void ingestThroughput(Blackhole bh) {
        Optional<Path> saved = Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        bh.consume(saved.isPresent());
        saved.ifPresent(bh::consume);
    }

    // ---------- Metrics helpers ----------

    private void recordSystemUsageBefore() {
        lastCpuBefore = safeCpuPercent();
        lastUsedMbBefore = usedMemoryMb();
    }

    private void recordSystemUsageAfter() {
        lastCpuAfter = safeCpuPercent();
        lastUsedMbAfter = usedMemoryMb();
    }

    private static double safeCpuPercent() {
        double v = osBean.getProcessCpuLoad(); // 0..1 or -1 if not available
        if (v < 0) return Double.NaN;
        return v * 100.0;
    }

    private static double usedMemoryMb() {
        long freeMem = Runtime.getRuntime().freeMemory();
        long totalMem = Runtime.getRuntime().totalMemory();
        long usedMem = totalMem - freeMem;
        return usedMem / (1024.0 * 1024.0);
    }

    // Optional getters if you want to print these in a separate runner/main after JMH
    public double getLastCpuBefore() { return lastCpuBefore; }
    public double getLastCpuAfter() { return lastCpuAfter; }
    public double getLastUsedMbBefore() { return lastUsedMbBefore; }
    public double getLastUsedMbAfter() { return lastUsedMbAfter; }
}
