package com.stage3.benchmarks;

import com.google.gson.Gson;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

final class BenchmarkOperations {
    private final Gson gson;
    private final HttpClient client;

    BenchmarkOperations(Gson gson) {
        this.gson = gson;
        this.client = HttpClient.newHttpClient();
    }

    Map<String, Object> runIngestion(EndpointPool pool, List<Integer> bookIds, int concurrency) throws Exception {
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

    Map<String, Object> runIndexing(EndpointPool pool, Map<String, Object> ingestionResult, int concurrency) throws Exception {
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

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        Queue<Map<String, Object>> indexedResponses = new ConcurrentLinkedQueue<>();
        Queue<Long> latencies = new ConcurrentLinkedQueue<>();

        Instant start = Instant.now();
        List<Future<Void>> futures = new ArrayList<>();

        for (Map<String, Object> response : responses) {
            Object bookIdRaw = response.get("book_id");
            if (!(bookIdRaw instanceof Number)) {
                continue;
            }
            int bookId = ((Number) bookIdRaw).intValue();
            String lakePath = response.get("path") + "/" + response.get("file");
            futures.add(executor.submit(() -> {
                String endpoint = pool.next();
                Map<String, Object> payload = new HashMap<>();
                payload.put("lakePath", lakePath.replace("\\", "/"));
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoint + "/index"))
                        .timeout(Duration.ofSeconds(120))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(payload)))
                        .build();
                Instant requestStart = Instant.now();
                HttpResponse<String> responseIndex = client.send(request, HttpResponse.BodyHandlers.ofString());
                latencies.add(Duration.between(requestStart, Instant.now()).toMillis());
                Map<String, Object> responsePayload = parseJson(responseIndex.body());
                responsePayload.put("bookId", bookId);
                responsePayload.put("lakePath", lakePath);
                responsePayload.put("endpoint", endpoint);
                responsePayload.put("http_status", responseIndex.statusCode());
                indexedResponses.add(responsePayload);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        long durationMs = Duration.between(start, Instant.now()).toMillis();
        int tokensTotal = 0;
        for (Map<String, Object> response : indexedResponses) {
            Object tokens = response.get("tokensTotal");
            if (tokens instanceof Number) {
                tokensTotal += ((Number) tokens).intValue();
            }
        }
        double tokensPerSecond = durationMs > 0 ? (tokensTotal * 1000.0 / durationMs) : 0.0;

        LatencyStats stats = LatencyStats.from(latencies);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("requested", indexedResponses.size());
        result.put("successful", indexedResponses.size());
        result.put("duration_ms", durationMs);
        result.put("tokens_total", tokensTotal);
        result.put("tokens_per_second", tokensPerSecond);
        result.put("latency_avg_ms", stats.averageMs);
        result.put("latency_p95_ms", stats.p95Ms);
        result.put("latency_max_ms", stats.maxMs);
        result.put("responses", indexedResponses);
        return result;
    }

    Map<String, Object> runSearchLoad(EndpointPool pool, List<String> queries, int concurrency, int durationSeconds) throws Exception {
        waitForSearchReady(pool, queries, Duration.ofSeconds(60));
        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        Queue<Long> latencies = new ConcurrentLinkedQueue<>();
        Queue<Integer> statuses = new ConcurrentLinkedQueue<>();

        Instant start = Instant.now();
        Instant end = start.plusSeconds(durationSeconds);
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
                    latencies.add(Duration.between(requestStart, Instant.now()).toMillis());
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

    private void waitForSearchReady(EndpointPool pool, List<String> queries, Duration timeout) throws Exception {
        if (queries.isEmpty()) {
            return;
        }
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

    long waitForRecovery(List<String> healthUrls, int timeoutSeconds, int pollIntervalSeconds) throws Exception {
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

    Map<String, Object> parseJson(String body) {
        if (body == null || body.isBlank()) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Object> parsed = gson.fromJson(body, Map.class);
            return parsed != null ? parsed : new LinkedHashMap<>();
        } catch (Exception ex) {
            Map<String, Object> fallback = new LinkedHashMap<>();
            fallback.put("raw", body);
            return fallback;
        }
    }

    private Map<Integer, Integer> summarizeStatuses(Queue<Integer> statuses) {
        Map<Integer, Integer> summary = new HashMap<>();
        for (Integer status : statuses) {
            summary.merge(status, 1, Integer::sum);
        }
        return summary;
    }
}
