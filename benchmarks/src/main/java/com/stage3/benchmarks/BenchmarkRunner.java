package com.stage3.benchmarks;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class BenchmarkRunner {
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final DateTimeFormatter RUN_ID_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }

        String scenario = args[0].toLowerCase(Locale.ROOT);
        Map<String, String> options = parseOptions(args);
        String configPath = options.get("config");
        String outputDir = options.getOrDefault("output-dir", "benchmark_results");

        if (options.containsKey("reset")) {
            String defaultReset = isWindows()
                    ? "benchmarks/reset_cluster.ps1"
                    : "benchmarks/reset_cluster.sh";
            runResetScript(options.getOrDefault("reset-script", defaultReset));
        }

        BenchmarkConfig config = BenchmarkConfig.load(configPath);
        Path outputPath = Path.of(outputDir);
        Files.createDirectories(outputPath);

        String runId = LocalDateTime.now().format(RUN_ID_FORMAT);

        switch (scenario) {
            case "baseline" -> runBaseline(config, outputPath, runId);
            case "scaling" -> runScaling(config, outputPath, runId);
            case "load" -> runLoad(config, outputPath, runId);
            case "failure" -> runFailure(config, outputPath, runId);
            default -> {
                System.err.println("Unknown scenario: " + scenario);
                printUsage();
                System.exit(1);
            }
        }
    }

    private static void runBaseline(BenchmarkConfig config, Path outputDir, String runId) throws Exception {
        BenchmarkConfig.BaselineScenario scenario = config.baseline;
        EndpointPool ingestionPool = EndpointPool.single(scenario.ingestionUrl);
        EndpointPool indexingPool = EndpointPool.single(scenario.indexingUrl);
        EndpointPool searchPool = EndpointPool.single(scenario.searchUrl);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "baseline");

        Map<String, Object> ingestionResult = runIngestion(ingestionPool, scenario.bookIds, scenario.ingestionConcurrency);
        Map<String, Object> indexingResult = runIndexing(indexingPool, ingestionResult, scenario.indexingConcurrency);
        Map<String, Object> searchResult = runSearchLoad(searchPool, scenario.queries, scenario.searchConcurrency, scenario.searchDurationSeconds);
        result.put("ingestion", ingestionResult);
        result.put("indexing", indexingResult);
        result.put("search", searchResult);
        result.put("system_stats", DockerStatsCollector.collect(config.dockerStats));

        writeResults(outputDir, "baseline", runId, result, Map.of());
    }

    private static void runScaling(BenchmarkConfig config, Path outputDir, String runId) throws Exception {
        BenchmarkConfig.ScalingScenario scenario = config.scaling;
        List<Map<String, Object>> scaleResults = new ArrayList<>();
        for (BenchmarkConfig.ScaleSet set : scenario.sets) {
            EndpointPool ingestionPool = EndpointPool.roundRobin(set.ingestionUrls);
            EndpointPool indexingPool = EndpointPool.roundRobin(set.indexingUrls);
            EndpointPool searchPool = EndpointPool.roundRobin(set.searchUrls);

            Map<String, Object> setResult = new LinkedHashMap<>();
            setResult.put("name", set.name);

            Map<String, Object> ingestionResult = runIngestion(ingestionPool, scenario.bookIds, scenario.ingestionConcurrency);
            Map<String, Object> indexingResult = runIndexing(indexingPool, ingestionResult, scenario.indexingConcurrency);
            Map<String, Object> searchResult = runSearchLoad(searchPool, scenario.queries, scenario.searchConcurrency, scenario.searchDurationSeconds);

            setResult.put("ingestion", ingestionResult);
            setResult.put("indexing", indexingResult);
            setResult.put("search", searchResult);
            setResult.put("system_stats", DockerStatsCollector.collect(config.dockerStats));
            scaleResults.add(setResult);

        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "scaling");
        result.put("sets", scaleResults);

        writeResults(outputDir, "scaling", runId, result, Map.of());
    }

    private static void runLoad(BenchmarkConfig config, Path outputDir, String runId) throws Exception {
        BenchmarkConfig.LoadScenario scenario = config.load;
        EndpointPool searchPool = EndpointPool.roundRobin(scenario.searchUrls);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "load");

        Map<String, Object> searchResult = runSearchLoad(searchPool, scenario.queries, scenario.concurrency, scenario.durationSeconds);
        result.put("search", searchResult);
        result.put("system_stats", DockerStatsCollector.collect(config.dockerStats));

        writeResults(outputDir, "load", runId, result, Map.of());
    }

    private static void runFailure(BenchmarkConfig config, Path outputDir, String runId) throws Exception {
        BenchmarkConfig.FailureScenario scenario = config.failure;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("scenario", "failure");
        result.put("failure_command", scenario.failureCommand);

        Instant start = Instant.now();
        Process process = new ProcessBuilder("bash", "-lc", scenario.failureCommand)
            .redirectErrorStream(true)
            .start();
        int exitCode = process.waitFor();
        result.put("failure_exit_code", exitCode);

        long recoveryTime = waitForRecovery(scenario.healthUrls, scenario.recoveryTimeoutSeconds, scenario.pollIntervalSeconds);
        result.put("recovery_time_seconds", recoveryTime >= 0 ? recoveryTime : null);
        result.put("recovered", recoveryTime >= 0);
        result.put("system_stats", DockerStatsCollector.collect(config.dockerStats));

        Map<String, Object> timings = new LinkedHashMap<>();
        timings.put("total_duration_seconds", Duration.between(start, Instant.now()).toSeconds());
        result.put("timings", timings);

        writeResults(outputDir, "failure", runId, result, Map.of());
    }

    private static Map<String, Object> runIngestion(EndpointPool pool, List<Integer> bookIds, int concurrency) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        Queue<Map<String, Object>> responses = new ConcurrentLinkedQueue<>();
        Queue<Long> latencies = new ConcurrentLinkedQueue<>();

        Instant start = Instant.now();
        List<Future<Void>> futures = new ArrayList<>();

        for (Integer bookId : bookIds) {
            futures.add(executor.submit(() -> {
                String endpoint = pool.next();
                String url = endpoint + "/ingest/" + bookId;
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(120))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
                Instant requestStart = Instant.now();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                latencies.add(Duration.between(requestStart, Instant.now()).toMillis());
                Map<String, Object> payload = parseJson(response.body());
                payload.put("http_status", response.statusCode());
                payload.put("endpoint", endpoint);
                responses.add(payload);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        long durationMs = Duration.between(start, Instant.now()).toMillis();
        double docsPerSecond = durationMs > 0 ? (bookIds.size() * 1000.0 / durationMs) : 0.0;

        List<Map<String, Object>> successful = new ArrayList<>();
        for (Map<String, Object> payload : responses) {
            if ("downloaded".equals(payload.get("status"))) {
                successful.add(payload);
            }
        }

        LatencyStats stats = LatencyStats.from(latencies);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("requested", bookIds.size());
        result.put("successful", successful.size());
        result.put("duration_ms", durationMs);
        result.put("docs_per_second", docsPerSecond);
        result.put("latency_avg_ms", stats.averageMs);
        result.put("latency_p95_ms", stats.p95Ms);
        result.put("latency_max_ms", stats.maxMs);
        result.put("responses", successful);
        return result;
    }

    private static Map<String, Object> runIndexing(EndpointPool pool, Map<String, Object> ingestionResult, int concurrency) throws Exception {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> responses = (List<Map<String, Object>>) ingestionResult.get("responses");
        if (responses.isEmpty()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("requested", 0);
            result.put("successful", 0);
            result.put("duration_ms", 0);
            result.put("tokens_per_second", 0.0);
            result.put("latencies_ms", List.of());
            result.put("responses", List.of());
            return result;
        }

        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        Queue<Map<String, Object>> indexResponses = new ConcurrentLinkedQueue<>();
        Queue<Long> latencies = new ConcurrentLinkedQueue<>();

        Instant start = Instant.now();
        List<Future<Void>> futures = new ArrayList<>();

        for (Map<String, Object> payload : responses) {
            futures.add(executor.submit(() -> {
                boolean eventSent = isEventSent(payload.get("event_sent"));
                String endpoint = pool.next();
                if (eventSent) {
                    Map<String, Object> responsePayload = waitForIndexingByMetadata(client, endpoint, payload);
                    indexResponses.add(responsePayload);
                    latencies.add((Long) responsePayload.getOrDefault("latency_ms", 0L));
                    return null;
                }

                String lakePath = payload.get("path") + "/" + payload.get("file");
                String url = endpoint + "/index";
                String body = GSON.toJson(Map.of("lakePath", lakePath));
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(120))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
                Instant requestStart = Instant.now();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                latencies.add(Duration.between(requestStart, Instant.now()).toMillis());
                Map<String, Object> responsePayload = parseJson(response.body());
                responsePayload.put("http_status", response.statusCode());
                responsePayload.put("endpoint", endpoint);
                indexResponses.add(responsePayload);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        long durationMs = Duration.between(start, Instant.now()).toMillis();
        long totalTokens = 0L;
        List<Map<String, Object>> successful = new ArrayList<>();
        for (Map<String, Object> payload : indexResponses) {
            Object status = payload.get("status");
            Object httpStatus = payload.get("http_status");
            boolean ok = "ok".equals(status) || "already_indexed".equals(status);
            if (!ok && httpStatus instanceof Number) {
                ok = ((Number) httpStatus).intValue() == 200;
            }
            if (ok) {
                successful.add(payload);
                Number tokens = (Number) payload.get("tokensTotal");
                if (tokens != null) {
                    totalTokens += tokens.longValue();
                }
            }
        }

        double tokensPerSecond = durationMs > 0 ? (totalTokens * 1000.0 / durationMs) : 0.0;

        LatencyStats stats = LatencyStats.from(latencies);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("requested", responses.size());
        result.put("successful", successful.size());
        result.put("duration_ms", durationMs);
        result.put("tokens_total", totalTokens);
        result.put("tokens_per_second", tokensPerSecond);
        result.put("latency_avg_ms", stats.averageMs);
        result.put("latency_p95_ms", stats.p95Ms);
        result.put("latency_max_ms", stats.maxMs);
        result.put("responses", successful);
        return result;
    }

    private static Map<String, Object> runSearchLoad(EndpointPool pool, List<String> queries, int concurrency, int durationSeconds) throws Exception {
        waitForSearchReady(pool, queries, Duration.ofSeconds(60));
        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        Queue<Long> latencies = new ConcurrentLinkedQueue<>();
        Queue<Integer> statuses = new ConcurrentLinkedQueue<>();

        Instant end = Instant.now().plusSeconds(durationSeconds);
        List<Future<Void>> futures = new ArrayList<>();

        for (int i = 0; i < concurrency; i++) {
            futures.add(executor.submit(() -> {
                while (Instant.now().isBefore(end)) {
                    String endpoint = pool.next();
                    String query = queries.get(ThreadLocalRandom.current().nextInt(queries.size()));
                    String url = endpoint + "/search?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8);
                    HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build();
                    Instant requestStart = Instant.now();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    long latency = Duration.between(requestStart, Instant.now()).toMillis();
                    latencies.add(latency);
                    statuses.add(response.statusCode());
                }
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        LatencyStats stats = LatencyStats.from(latencies);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("requests", latencies.size());
        result.put("avg_ms", stats.averageMs);
        result.put("p95_ms", stats.p95Ms);
        result.put("max_ms", stats.maxMs);
        result.put("status_codes", summarizeStatuses(statuses));
        return result;
    }

    private static void waitForSearchReady(EndpointPool pool, List<String> queries, Duration timeout) throws Exception {
        if (queries.isEmpty()) {
            return;
        }
        HttpClient client = HttpClient.newHttpClient();
        Instant deadline = Instant.now().plus(timeout);
        String query = queries.get(0);

        while (Instant.now().isBefore(deadline)) {
            String endpoint = pool.next();
            String url = endpoint + "/search?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }
    }

    private static boolean isEventSent(Object raw) {
        if (raw == null) {
            return false;
        }
        if (raw instanceof Boolean) {
            return (Boolean) raw;
        }
        return "true".equalsIgnoreCase(raw.toString());
    }

    private static Map<String, Object> waitForIndexingByMetadata(HttpClient client, String endpoint, Map<String, Object> payload) throws Exception {
        Map<String, Object> responsePayload = new LinkedHashMap<>();
        Object idRaw = payload.get("book_id");
        Integer bookId = idRaw instanceof Number ? ((Number) idRaw).intValue() : null;
        String lakePath = payload.get("path") + "/" + payload.get("file");

        responsePayload.put("bookId", bookId);
        responsePayload.put("lakePath", lakePath);
        responsePayload.put("endpoint", endpoint);

        if (bookId == null) {
            responsePayload.put("status", "error");
            responsePayload.put("http_status", 400);
            responsePayload.put("latency_ms", 0L);
            return responsePayload;
        }

        Instant start = Instant.now();
        Instant deadline = start.plusSeconds(120);

        while (Instant.now().isBefore(deadline)) {
            String url = endpoint + "/metadata/" + bookId;
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    Map<String, Object> md = parseJson(response.body());
                    Object status = md.get("status");
                    if ("INDEXED".equals(status)) {
                        responsePayload.put("status", "ok");
                        responsePayload.put("http_status", 200);
                        responsePayload.put("tokensTotal", md.get("tokenCount"));
                        responsePayload.put("latency_ms", Duration.between(start, Instant.now()).toMillis());
                        return responsePayload;
                    }
                    if ("FAILED".equals(status)) {
                        responsePayload.put("status", "failed");
                        responsePayload.put("http_status", 500);
                        responsePayload.put("latency_ms", Duration.between(start, Instant.now()).toMillis());
                        return responsePayload;
                    }
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }

        responsePayload.put("status", "timeout");
        responsePayload.put("http_status", 408);
        responsePayload.put("latency_ms", Duration.between(start, Instant.now()).toMillis());
        return responsePayload;
    }

    private static long waitForRecovery(List<String> healthUrls, int timeoutSeconds, int pollIntervalSeconds) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        Instant start = Instant.now();
        Instant deadline = Instant.now().plusSeconds(timeoutSeconds);

        while (Instant.now().isBefore(deadline)) {
            boolean healthy = true;
            for (String url : healthUrls) {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
                try {
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() >= 400) {
                        healthy = false;
                        break;
                    }
                } catch (IOException | InterruptedException ex) {
                    healthy = false;
                    break;
                }
            }
            if (healthy) {
                return Duration.between(start, Instant.now()).toSeconds();
            }
            Thread.sleep(pollIntervalSeconds * 1000L);
        }
        return -1;
    }

    private static Map<String, Object> parseJson(String body) {
        if (body == null || body.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> parsed = GSON.fromJson(body, Map.class);
            return parsed != null ? parsed : new LinkedHashMap<>();
        } catch (Exception ex) {
            Map<String, Object> fallback = new LinkedHashMap<>();
            fallback.put("raw", body);
            return fallback;
        }
    }

    private static Map<Integer, Integer> summarizeStatuses(Queue<Integer> statuses) {
        Map<Integer, Integer> summary = new HashMap<>();
        for (Integer status : statuses) {
            summary.merge(status, 1, Integer::sum);
        }
        return summary;
    }

    private static void writeResults(Path outputDir, String scenario, String runId, Map<String, Object> result, Map<String, Object> searchResult) throws IOException {
        Path jsonPath = outputDir.resolve(scenario + "-" + runId + ".json");
        Files.writeString(jsonPath, GSON.toJson(result), StandardCharsets.UTF_8);

        // Only summary metrics are persisted to keep result files small.
    }

    private static void writeCsv(Path path, List<Map<String, Object>> rows) throws IOException {
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

    private static List<Long> castLatencyList(Object raw) {
        if (raw instanceof List) {
            List<?> list = (List<?>) raw;
            List<Long> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Number) {
                    result.add(((Number) item).longValue());
                }
            }
            return result;
        }
        return List.of();
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String value = i + 1 < args.length && !args[i + 1].startsWith("--") ? args[++i] : "true";
                options.put(key, value);
            }
        }
        return options;
    }

    private static void printUsage() {
        System.out.println("Usage: java -jar benchmarks.jar <baseline|scaling|load|failure> [--config path] [--output-dir path] [--reset] [--reset-script path]");
    }

    private static void runResetScript(String scriptPath) throws IOException, InterruptedException {
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

    private static boolean isWindows() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        return os.contains("win");
    }

    private static final class EndpointPool {
        private final List<String> endpoints;
        private final AtomicInteger index = new AtomicInteger(0);

        private EndpointPool(List<String> endpoints) {
            this.endpoints = endpoints;
        }

        static EndpointPool single(String endpoint) {
            return new EndpointPool(List.of(endpoint));
        }

        static EndpointPool roundRobin(List<String> endpoints) {
            return new EndpointPool(endpoints);
        }

        String next() {
            int pos = Math.abs(index.getAndIncrement() % endpoints.size());
            return endpoints.get(pos);
        }
    }

    private static final class LatencyStats {
        private final double averageMs;
        private final long p95Ms;
        private final long maxMs;

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
}
