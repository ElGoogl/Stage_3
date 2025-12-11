package com.example.crawler;

import io.javalin.Javalin;
import com.google.gson.Gson;

import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class App {
    private static final Gson gson = new Gson();
    private static final Path DATA_DIR = Paths.get(
            System.getProperty("dataRepo", "data_repository/datalake_v1")
    );


    public static void main(String[] args) {

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(7001);

        if (!Files.exists(DATA_DIR)) {
            try {
                Files.createDirectories(DATA_DIR);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            System.out.println("[INIT] Created datalake directory at: " + DATA_DIR.toAbsolutePath());
        }

        app.post("/ingest/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            boolean ok = Downloader.downloadAndSave(bookId, DATA_DIR);
            if (ok) {
                var now = LocalDateTime.now();
                String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String hour = String.format("%02d", now.getHour());
                Map<String, Object> resp = Map.of(
                        "book_id", bookId,
                        "status", "downloaded",
                        "path", "data_repository/datalake_v1/" + date + "/" + hour
                );
                ctx.result(gson.toJson(resp));
            } else {
                ctx.status(500).result(gson.toJson(Map.of("book_id", bookId, "status", "failed")));
            }
        });

        app.get("/ingest/status/{book_id}", ctx -> {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            boolean exists = Files.exists(DATA_DIR.resolve(bookId + ".json"));
            Map<String, Object> resp = Map.of(
                    "book_id", bookId,
                    "status", exists ? "available" : "missing"
            );
            ctx.result(gson.toJson(resp));
        });

        app.get("/ingest/list", ctx -> {
            try (var stream = Files.list(DATA_DIR)) {
                List<Integer> ids = stream
                        .filter(p -> p.getFileName().toString().endsWith(".json"))
                        .map(p -> Integer.parseInt(p.getFileName().toString().replace(".json", "")))
                        .toList();
                ctx.result(gson.toJson(Map.of("count", ids.size(), "books", ids)));
            }
        });
    }
}
