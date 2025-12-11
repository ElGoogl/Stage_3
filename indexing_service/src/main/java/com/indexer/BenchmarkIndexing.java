package com.indexer;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

// imports ergänzen
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;


/**
 * Indexes exactly one book per invocation.
 * Default book set is the range 1342..1350.
 * You can override via -p bookIds="1342-1350,2701,2710-2712".
 */
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Threads(1)
public class BenchmarkIndexing {

    private IndexService indexService;
    private GlobalIndexer globalIndexer;

    // Default to the range 1342..1350
    @Param({"1342-1350"})
    public String bookIds;

    private List<Integer> bookIdList;
    private AtomicInteger rrIndex;

    @Setup(Level.Trial)
    public void setup() {
        indexService = new IndexService();
        globalIndexer = new GlobalIndexer();

        bookIdList = parseIds(bookIds);
        if (bookIdList.isEmpty()) {
            throw new IllegalStateException("No valid book IDs parsed from: " + bookIds);
        }
        rrIndex = new AtomicInteger(0);
    }

    // Latency per book
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureLatency(Blackhole bh) throws Exception {
        int id = nextId();
        indexService.buildIndex(id);
        long entries = countEntriesSafe(id);
        bh.consume(entries);
    }

    // Throughput in books per second
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureThroughput(Blackhole bh) throws Exception {
        int id = nextId();
        indexService.buildIndex(id);
        long entries = countEntriesSafe(id);
        bh.consume(entries);
    }

    private int nextId() {
        int i = rrIndex.getAndIncrement();
        return bookIdList.get(i % bookIdList.size());
    }

    private long countEntriesSafe(int bookId) {
        try {
            return globalIndexer.countEntries(bookId);
        } catch (Exception e) {
            return -1L;
        }
    }

    /**
     * Parses CSV of integers and ranges like "1342-1350,2701,2710-2712"
     */
    private static List<Integer> parseIds(String spec) {
        if (spec == null || spec.isBlank()) return List.of();
        List<Integer> out = new ArrayList<>();
        for (String part : spec.split(",")) {
            String s = part.trim();
            if (s.isEmpty()) continue;
            int dash = s.indexOf('-');
            if (dash > 0) {
                int a = Integer.parseInt(s.substring(0, dash).trim());
                int b = Integer.parseInt(s.substring(dash + 1).trim());
                int start = Math.min(a, b);
                int end = Math.max(a, b);
                for (int x = start; x <= end; x++) out.add(x);
            } else {
                out.add(Integer.parseInt(s));
            }
        }
        return out.stream().sorted().collect(Collectors.toUnmodifiableList());
    }

    // in der Klasse ergänzen

    // optional: kontinuierlicher CSV Sampler
    @Param({"false"})
    public boolean enableSampler;

    private Thread samplerThread;
    private volatile boolean sampling;
    private PrintWriter samplerOut;

    // pro Iteration eine kurze Zusammenfassung
    @TearDown(Level.Iteration)
    public void logResourceUsage() {
        Runtime rt = Runtime.getRuntime();
        long usedMemMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);

        double proc = -1, sys = -1;
        try {
            OperatingSystemMXBean os =
                    (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            proc = os.getProcessCpuLoad() * 100.0;
            sys  = os.getSystemCpuLoad() * 100.0;
        } catch (Throwable ignored) {}

        System.out.printf(Locale.ROOT,
                "[METRICS] RAM used: %d MB | Process CPU: %.2f%% | System CPU: %.2f%%%n",
                usedMemMB, proc, sys);
    }

    @Setup(Level.Trial)
    public void setupSamplerIfEnabled() {
        if (!enableSampler) return;
        try {
            Files.createDirectories(Paths.get("target"));
            samplerOut = new PrintWriter(Files.newBufferedWriter(Paths.get("target/jmh_metrics.csv")));
            samplerOut.println("epoch_ms,used_mem_mb,proc_cpu_pct,sys_cpu_pct");
        } catch (IOException e) {
            System.out.println("[METRICS] CSV could not be opened, sampler disabled: " + e.getMessage());
            return;
        }

        sampling = true;
        samplerThread = new Thread(() -> {
            Runtime rt = Runtime.getRuntime();
            OperatingSystemMXBean os = null;
            try {
                os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            } catch (Throwable ignored) {}

            while (sampling) {
                long now = System.currentTimeMillis();
                long used = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                double p = -1, s = -1;
                if (os != null) {
                    try { p = os.getProcessCpuLoad() * 100.0; s = os.getSystemCpuLoad() * 100.0; }
                    catch (Throwable ignored) {}
                }
                samplerOut.printf(Locale.ROOT, "%d,%d,%.3f,%.3f%n", now, used, p, s);
                samplerOut.flush();
                try { Thread.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
            }
        }, "jmh-metrics");
        samplerThread.setDaemon(true);
        samplerThread.start();
    }

    @TearDown(Level.Trial)
    public void stopSampler() {
        sampling = false;
        if (samplerThread != null) {
            try { samplerThread.join(1000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
        if (samplerOut != null) {
            try { samplerOut.flush(); samplerOut.close(); } catch (Throwable ignored) {}
        }
    }
}
