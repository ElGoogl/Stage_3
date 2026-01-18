package com.stage3.benchmarks;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class DockerStatsCollector {
    private DockerStatsCollector() {
    }

    static Object collect(BenchmarkConfig.DockerStats config) {
        if (config == null || !config.enabled) {
            return Map.of("enabled", false);
        }

        List<String> command = new ArrayList<>();
        command.add("docker");
        command.add("stats");
        command.add("--no-stream");
        command.add("--format");
        command.add("{{.Name}},{{.CPUPerc}},{{.MemUsage}}");
        if (config.containers != null && !config.containers.isEmpty()) {
            command.addAll(config.containers);
        }

        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            List<Map<String, String>> stats = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", -1);
                    if (parts.length >= 3) {
                        Map<String, String> row = new LinkedHashMap<>();
                        row.put("container", parts[0]);
                        row.put("cpu", parts[1]);
                        row.put("memory", parts[2]);
                        stats.add(row);
                    }
                }
            }
            int exitCode = process.waitFor();
            return Map.of(
                "enabled", true,
                "exit_code", exitCode,
                "stats", stats
            );
        } catch (Exception ex) {
            return Map.of(
                "enabled", true,
                "error", ex.getMessage()
            );
        }
    }
}
