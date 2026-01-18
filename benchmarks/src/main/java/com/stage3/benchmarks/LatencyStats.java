package com.stage3.benchmarks;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;

final class LatencyStats {
    final double averageMs;
    final long p95Ms;
    final long maxMs;

    private LatencyStats(double averageMs, long p95Ms, long maxMs) {
        this.averageMs = averageMs;
        this.p95Ms = p95Ms;
        this.maxMs = maxMs;
    }

    static LatencyStats from(Queue<Long> latencies) {
        if (latencies.isEmpty()) {
            return new LatencyStats(0.0, 0, 0);
        }
        List<Long> sorted = new ArrayList<>(latencies);
        sorted.sort(Comparator.naturalOrder());
        long sum = 0L;
        for (Long value : sorted) {
            sum += value;
        }
        double avg = sum / (double) sorted.size();
        int p95Index = (int) Math.ceil(sorted.size() * 0.95) - 1;
        p95Index = Math.min(Math.max(p95Index, 0), sorted.size() - 1);
        long p95 = sorted.get(p95Index);
        long max = sorted.get(sorted.size() - 1);
        return new LatencyStats(avg, p95, max);
    }
}
