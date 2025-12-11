package com.example.benchmarks;

import com.example.crawler.Downloader;
import org.openjdk.jmh.annotations.*;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark measuring ingestion time, CPU and memory usage
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
    private static final int MAX_ID = 1400;

    private int getNextBookId() {
        if (nextBookId > MAX_ID) nextBookId = 1342;
        return nextBookId++;
    }

    /** Average time per single ingestion */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public boolean ingestOneBookAvgTime() {
        recordSystemUsage("Before");

        boolean ok = Downloader.downloadAndSave(getNextBookId(), DATA_DIR);

        recordSystemUsage("After");
        return ok;
    }

    /** Batch ingestion of 10 books, measuring CPU+Memory for the batch */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void ingestTenBooksCpuMem() {
        recordSystemUsage("BeforeBatch");

        for (int i = 0; i < 10; i++) {
            Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
        }

        recordSystemUsage("AfterBatch");
    }

    /** Throughput test */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public boolean ingestThroughput() {
        return Downloader.downloadAndSave(getNextBookId(), DATA_DIR);
    }

    /** Prints CPU and memory metrics */
    private void recordSystemUsage(String tag) {
        double processCpu = osBean.getProcessCpuLoad() * 100.0;
        long freeMem = Runtime.getRuntime().freeMemory();
        long totalMem = Runtime.getRuntime().totalMemory();
        long usedMem = totalMem - freeMem;
        double usedMB = usedMem / (1024.0 * 1024.0);

        System.out.printf("[%s] CPU: %.1f%% | Used Memory: %.1f MB%n", tag, processCpu, usedMB);
    }

}
