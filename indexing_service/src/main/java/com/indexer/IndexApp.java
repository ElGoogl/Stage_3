package com.indexer;

import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Indexing Service – Stage 2 Implementation
 *
 * Endpoints (as defined in Stage 2 project hints §4.2):
 *  POST /index/update/{book_id}   → Index one specific book
 *  POST /index/rebuild            → Index all books and rebuild the global index
 *  GET  /index/status             → Return indexing statistics
 *  GET  /status                   → Health check
 */
public class IndexApp {

    public static void main(String[] args) {

        // --- Initialize service dependencies ---
        IndexService indexService = new IndexService();
        GlobalIndexer globalIndexer = new GlobalIndexer();

        // --- Start Javalin server ---
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.http.defaultContentType = "application/json";
        }).start(7002);

        System.out.println("[INDEXER] Service started on http://localhost:7002");

        // ----------------------------------------------------------
        // GET /status → Health check
        // ----------------------------------------------------------
        app.get("/status", ctx -> ctx.json(Map.of(
                "service", "indexing_service",
                "status", "running"
        )));

        // ----------------------------------------------------------
        // POST /index/update/{book_id}
        // → Index one specific book
        // ----------------------------------------------------------
        app.post("/index/update/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            Map<String, Object> result = indexService.buildIndex(bookId);

            // After successful indexing, rebuild global index automatically
            if ("indexed".equals(result.get("status"))) {
                globalIndexer.buildGlobalIndex();
                System.out.println("[INDEXER] Global index automatically rebuilt after book " + bookId);
            }

            ctx.json(result);
        });

        // ----------------------------------------------------------
        // POST /index/rebuild
        // → Index all books in datalake and rebuild the global index
        // ----------------------------------------------------------
        app.post("/index/rebuild", ctx -> {
            System.out.println("[INDEXER] Starting full rebuild of all books...");

            int indexedCount = 0;
            java.nio.file.Path datalake = java.nio.file.Paths.get("data_repository/datalake_v1/");

            try (java.nio.file.DirectoryStream<java.nio.file.Path> stream =
                         java.nio.file.Files.newDirectoryStream(datalake, "*.json")) {

                for (java.nio.file.Path file : stream) {
                    String filename = file.getFileName().toString();
                    int bookId = Integer.parseInt(filename.replace(".json", ""));
                    Map<String, Object> result = indexService.buildIndex(bookId);

                    if ("indexed".equals(result.get("status")) || "already_indexed".equals(result.get("status"))) {
                        indexedCount++;
                        System.out.println("[INDEXER] Indexed book " + bookId + " successfully.");
                    } else {
                        System.out.println("[INDEXER] Skipped or failed: " + bookId + " (" + result.get("status") + ")");
                    }
                }

                // After processing all books, build the global index
                globalIndexer.buildGlobalIndex();
                System.out.println("[INDEXER] Global index rebuilt successfully after processing " + indexedCount + " books.");

                ctx.json(Map.of(
                        "status", "ok",
                        "message", "Full rebuild completed",
                        "books_processed", indexedCount,
                        "path", "data_repository/inverted_index.json"
                ));

            } catch (Exception e) {
                e.printStackTrace();
                ctx.json(Map.of(
                        "status", "error",
                        "message", e.getMessage()
                ));
            }
        });

        // ----------------------------------------------------------
        // GET /index/status
        // → Return indexing statistics
        // ----------------------------------------------------------
        app.get("/index/status", ctx -> {
            int booksIndexed = indexService.getIndexedCount();
            ctx.json(Map.of(
                    "books_indexed", booksIndexed,
                    "last_update", LocalDateTime.now().toString(),
                    "index_path", "data_repository/indexes/"
            ));
        });
    }
}