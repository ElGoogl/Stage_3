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
 *  POST /index/rebuild            → Rebuild full global index
 *  GET  /index/status             → Return indexing statistics
 *  GET  /status                   → Health check
 */
public class IndexApp {

    public static void main(String[] args) {

        // --- Initialize service dependencies ---
        IndexService indexService = new IndexService();
        GlobalIndexer globalIndexer = new GlobalIndexer();

        // --- Optional local test: manual single-book indexing ---
        boolean runLocalTest = true; // set to false when using the Control Module

        if (runLocalTest) {
            System.out.println("[DEBUG] Running local indexing test for book 1342...");
            Map<String, Object> result42 = indexService.buildIndex(1342);
            System.out.println("[DEBUG] Manual test result: " + result42);
        }

        // --- Start Javalin server ---
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson());
            config.http.defaultContentType = "application/json";
        }).start(7002);

        System.out.println("[INDEXER] Service started on http://localhost:7002");

        //  GET /status → Health check
        app.get("/status", ctx -> ctx.json(Map.of(
                "service", "indexing_service",
                "status", "running"
        )));

        app.post("/index/update/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            Map<String, Object> result = indexService.buildIndex(bookId);

            // --- After successful indexing, rebuild the global index ---
            if ("indexed".equals(result.get("status"))) {
                globalIndexer.buildGlobalIndex();
                System.out.println("[INDEXER] Global index automatically rebuilt after book " + bookId);
            }

            ctx.json(result);
        });


        // POST /index/rebuild
        //     → Rebuild global inverted index
        app.post("/index/rebuild", ctx -> {
            globalIndexer.buildGlobalIndex();
            ctx.json(Map.of(
                    "status", "ok",
                    "message", "Global index rebuilt successfully",
                    "path", "data_repository/inverted_index.json"
            ));
        });

        // GET /index/status
        //     → Return indexing statistics
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
