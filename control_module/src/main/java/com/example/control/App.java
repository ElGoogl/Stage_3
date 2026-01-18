package com.example.control;

import io.javalin.Javalin;
import com.google.gson.Gson;
import java.net.http.*;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class App {

    private static final Gson gson = new Gson();
    private static final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    // adjust to actual service URL once they are all ready
    private static final String INGEST_URL = "http://localhost:7001/ingest/";
    private static final String INDEX_URL  = "http://localhost:7002/index/update/";
    private static final String SEARCH_URL = "http://localhost:7003/search?q=";

    public static void main(String[] args) {

        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String reindexQueue = System.getenv().getOrDefault("ACTIVEMQ_REINDEX_QUEUE", "books.reindex");
        ActiveMqPublisher reindexPublisher = new ActiveMqPublisher(brokerUrl, reindexQueue);

        Javalin app = Javalin.create(config -> config.http.defaultContentType = "application/json")
                .start(7000);

        System.out.println("[CONTROL] Service started on http://localhost:7000");

        app.get("/status", ctx -> {
            ctx.json(Map.of("service", "control-panel", "status", "running"));
        });

        // trigger Ingestion
        app.post("/control/ingest/{id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("id"));
            String result = sendRequest(INGEST_URL + bookId, "POST");
            ctx.result(result);
        });

        // trigger Indexing
        app.post("/control/index/{id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("id"));
            String result = sendRequest(INDEX_URL + bookId, "POST");
            ctx.result(result);
        });

        // trigger control
        app.get("/control/search", ctx -> {
            String query = ctx.queryParam("q");
            String result = sendRequest(SEARCH_URL + query, "GET");
            ctx.result(result);
        });

        // trigger cluster-wide reindex request via broker
        app.post("/control/reindex", ctx -> {
            String lakePath = ctx.queryParam("lakePath");
            Integer bookId = null;

            if (lakePath == null || lakePath.isBlank()) {
                try {
                    Map<?, ?> body = gson.fromJson(ctx.body(), Map.class);
                    if (body != null) {
                        Object lp = body.get("lakePath");
                        Object id = body.get("bookId");
                        lakePath = lp != null ? lp.toString() : null;
                        if (id != null) {
                            try {
                                bookId = Integer.parseInt(id.toString());
                            } catch (NumberFormatException ignored) {
                            }
                        }
                    }
                } catch (Exception ignored) {
                }
            }

            if (lakePath == null || lakePath.isBlank()) {
                ctx.status(400).json(Map.of("error", "lakePath is required"));
                return;
            }

            Map<String, Object> event = new HashMap<>();
            event.put("eventType", "reindex_request");
            event.put("lakePath", lakePath);
            event.put("requestedAt", Instant.now().toString());
            if (bookId != null) {
                event.put("bookId", bookId);
            }

            reindexPublisher.publish(gson.toJson(event));
            ctx.json(Map.of("status", "queued", "queue", reindexQueue, "lakePath", lakePath));
        });

        // start whole pipeline (ingestion + indexing)
        app.post("/control/pipeline/{id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("id"));
            System.out.println("[CONTROL] Starting pipeline for book " + bookId);

            // Ingestion phase
            String statusJson = sendRequest(INGEST_URL + "status/" + bookId, "GET");
            Map<?, ?> status = gson.fromJson(statusJson, Map.class);
            boolean alreadyIngested = "available".equals(status.get("status"));

            String ingestResult;
            if (!alreadyIngested) {
                System.out.println("[CONTROL] Book not yet ingested, starting ingestion...");
                ingestResult = sendRequest(INGEST_URL + bookId, "POST");
            } else {
                System.out.println("[CONTROL] Book already ingested, skipping download.");
                ingestResult = gson.toJson(Map.of("status", "skipped"));
            }

            // verify ingestion completion
            boolean ready = false;
            for (int i = 0; i < 10; i++) {
                String check = sendRequest(INGEST_URL + "status/" + bookId, "GET");
                if (check.contains("available")) { ready = true; break; }
                Thread.sleep(1000);
            }

            // indexing phase
            String indexResult;
            if (ready) {
                // 💡 Check index status before starting
                String indexStatusJson = sendRequest("http://localhost:7002/index/status/" + bookId, "GET");
                Map<?, ?> indexStatus = gson.fromJson(indexStatusJson, Map.class);
                boolean alreadyIndexed = "indexed".equals(indexStatus.get("status"));

                if (alreadyIndexed) {
                    System.out.println("[CONTROL] Book already indexed, skipping indexing.");
                    indexResult = gson.toJson(Map.of("status", "skipped"));
                } else {
                    System.out.println("[CONTROL] Ingestion confirmed. Starting indexing...");
                    indexResult = sendRequest(INDEX_URL + bookId, "POST");
                    System.out.println("[CONTROL] Indexing confirmed. Pipeline finished.");
                }
            } else {
                indexResult = gson.toJson(Map.of("status", "ingestion_failed"));
            }

            ctx.json(Map.of(
                    "book_id", bookId,
                    "ingest", gson.fromJson(ingestResult, Object.class),
                    "index", gson.fromJson(indexResult, Object.class)
            ));
        });
    }

    private static String sendRequest(String url, String method) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(30))
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return response.body();
        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson(Map.of("error", e.getMessage(), "url", url));
        }
    }
}
