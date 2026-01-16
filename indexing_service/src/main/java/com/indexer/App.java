package com.indexer;

import com.google.gson.Gson;
import com.indexer.core.BookParser;
import com.indexer.core.IndexService;
import com.indexer.core.PathResolver;
import com.indexer.core.Tokenizer;
import com.indexer.hz.HazelcastProvider;
import com.indexer.index.ClaimStore;
import com.indexer.index.IndexedStore;
import com.indexer.index.InvertedIndexStore;
import com.indexer.mq.ActiveMqIngestConsumer;
import com.indexer.web.IndexController;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class App {

    public static Javalin start(int httpPort, Path lakeRoot, Path indexRoot) {
        ensureDirExists(indexRoot);

        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "localhost:5701");
        String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "stage3");
        String hzNode = System.getenv().getOrDefault("NODE_ID", "indexer_" + httpPort);

        HazelcastProvider hzProvider = new HazelcastProvider(hzMembers, hzCluster, hzNode);

        InvertedIndexStore invertedIndex = new InvertedIndexStore(hzProvider.instance());
        ClaimStore claimStore = new ClaimStore(hzProvider.instance());
        IndexedStore indexedStore = new IndexedStore(hzProvider.instance());

        Gson gson = new Gson();
        BookParser bookParser = new BookParser(gson);
        Tokenizer tokenizer = new Tokenizer();

        PathResolver resolver = new PathResolver(lakeRoot);
        IndexService indexService = new IndexService(
                resolver,
                indexRoot,
                claimStore,
                invertedIndex,
                indexedStore,
                bookParser,
                tokenizer
        );

        IndexController indexController = new IndexController(gson, indexService);

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json");

        indexController.registerRoutes(app);

        app.post("/hz/smoke", ctx -> {
            String term = java.util.Optional.ofNullable(ctx.queryParam("term")).orElse("hola");
            String idRaw = java.util.Optional.ofNullable(ctx.queryParam("id")).orElse("123");
            int id = Integer.parseInt(idRaw);

            invertedIndex.put(term, id);

            ctx.result(gson.toJson(java.util.Map.of(
                    "status", "ok",
                    "term", term,
                    "count", invertedIndex.valueCount(term),
                    "docs", invertedIndex.get(term)
            )));
        });

        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String queueName = System.getenv().getOrDefault("ACTIVEMQ_QUEUE", "books.ingested");

        ActiveMqIngestConsumer mqConsumer = new ActiveMqIngestConsumer(gson, indexService, brokerUrl, queueName);
        mqConsumer.start();

        app.events(ev -> ev.serverStopping(() -> {
            try { mqConsumer.close(); } catch (Exception ignored) {}
            hzProvider.shutdown();
        }));

        app.start(httpPort);
        return app;
    }

    private static void ensureDirExists(Path dir) {
        if (Files.exists(dir)) return;
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create dir: " + dir, e);
        }
    }

    public static void main(String[] args) {
        int httpPort = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));

        // IMPORTANT: match ingestion_service DATA_DIR default (datalake_v1)
        Path lakeRoot = Path.of("data_repository", "datalake_v1").normalize();
        Path indexRoot = Path.of("data_repository", "indexes").normalize();

        start(httpPort, lakeRoot, indexRoot);
    }
}