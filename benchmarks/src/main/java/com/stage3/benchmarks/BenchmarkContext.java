package com.stage3.benchmarks;

import com.google.gson.Gson;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class BenchmarkContext {
    final BenchmarkConfig config;
    final Path outputDir;
    final String runId;
    final Gson gson;
    final BenchmarkOperations ops;

    BenchmarkContext(BenchmarkConfig config, Path outputDir, String runId, Gson gson) {
        this.config = config;
        this.outputDir = outputDir;
        this.runId = runId;
        this.gson = gson;
        this.ops = new BenchmarkOperations(gson);
    }

    void writeResults(String scenario, Map<String, Object> result) throws IOException {
        Path jsonPath = outputDir.resolve(scenario + "-" + runId + ".json");
        Files.writeString(jsonPath, gson.toJson(result), StandardCharsets.UTF_8);
    }

    void writeCsv(Path path, List<Map<String, Object>> rows) throws IOException {
        if (rows.isEmpty()) {
            return;
        }
        List<String> headers = new ArrayList<>(rows.get(0).keySet());
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(String.join(",", headers));
            writer.newLine();
            for (Map<String, Object> row : rows) {
                List<String> values = new ArrayList<>();
                for (String header : headers) {
                    Object value = row.get(header);
                    values.add(value == null ? "" : value.toString());
                }
                writer.write(String.join(",", values));
                writer.newLine();
            }
        }
    }

    void waitForIngestionReady(List<String> ingestionUrls) throws Exception {
        if (ingestionUrls == null || ingestionUrls.isEmpty()) {
            return;
        }
        int attempts = 30;
        int waitSeconds = 2;

        List<String> pending = new ArrayList<>(ingestionUrls);
        for (int attempt = 1; attempt <= attempts && !pending.isEmpty(); attempt++) {
            List<String> nextPending = new ArrayList<>();
            for (String baseUrl : pending) {
                String url = baseUrl.replaceAll("/+$", "") + "/ingest/list";
                try {
                    if (ops.waitForRecovery(List.of(url), 1, 1) != 0) {
                        nextPending.add(baseUrl);
                    }
                } catch (Exception e) {
                    nextPending.add(baseUrl);
                }
            }
            pending = nextPending;
            if (!pending.isEmpty()) {
                Thread.sleep(waitSeconds * 1000L);
            }
        }

        if (!pending.isEmpty()) {
            throw new IllegalStateException("Ingestion endpoints not ready: " + pending);
        }
    }

    List<String> collectBaselineIngestionUrls() {
        List<String> urls = new ArrayList<>();
        if (config.baseline != null && config.baseline.ingestionUrl != null) {
            urls.add(config.baseline.ingestionUrl);
        }
        if (config.scaling != null && config.scaling.sets != null) {
            for (BenchmarkConfig.ScaleSet set : config.scaling.sets) {
                if (set.ingestionUrls != null) {
                    urls.addAll(set.ingestionUrls);
                }
            }
        }
        return urls.stream().distinct().toList();
    }

    void runResetScript(String scriptPath) throws IOException, InterruptedException {
        Path script = Path.of(scriptPath).normalize();
        if (!Files.isRegularFile(script)) {
            throw new IOException("Reset script not found: " + script.toAbsolutePath());
        }

        List<String> command = new ArrayList<>();
        if (isWindows() || script.toString().toLowerCase(Locale.ROOT).endsWith(".ps1")) {
            command.add("powershell");
            command.add("-ExecutionPolicy");
            command.add("Bypass");
            command.add("-File");
            command.add(script.toString());
        } else {
            command.add("bash");
            command.add(script.toString());
        }

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();
        Process proc = pb.start();
        int exit = proc.waitFor();
        if (exit != 0) {
            throw new IOException("Reset script failed with exit code: " + exit);
        }
    }

    static String defaultResetScript() {
        return isWindows() ? "benchmarks/reset_cluster.ps1" : "benchmarks/reset_cluster.sh";
    }

    private static boolean isWindows() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }
}
