package com.indexer;

import com.google.gson.Gson;
import com.indexer.core.*;
import com.indexer.hz.HazelcastProvider;
import com.indexer.index.*;
import com.indexer.messaging.ActiveMqIndexer;
import com.indexer.web.IndexController;
import com.indexer.web.MetadataController;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class App {

    public static Javalin start(int port, Path lakeRoot, Path indexRoot) {
        ensureDirExists(indexRoot);

        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String queueName = System.getenv().getOrDefault("ACTIVEMQ_QUEUE", "books.ingested");
        String reindexQueue = System.getenv().getOrDefault("ACTIVEMQ_REINDEX_QUEUE", "books.reindex");
        String indexedQueue = System.getenv().getOrDefault("ACTIVEMQ_INDEXED_QUEUE", "books.indexed");
        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
        String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "stage3");
        String hzNode = System.getenv().getOrDefault("NODE_ID", "indexer-" + port);

        HazelcastProvider hzProvider = new HazelcastProvider(hzMembers, hzCluster, hzNode);

        InvertedIndexStore invertedIndex = new InvertedIndexStore(hzProvider.instance());
        ClaimStore claimStore = new ClaimStore(hzProvider.instance());
        IndexedStore indexedStore = new IndexedStore(hzProvider.instance());
        DocumentMetadataStore metadataStore = new DocumentMetadataStore(hzProvider.instance());

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
                metadataStore,
                hzNode,
                bookParser,
                tokenizer
        );

        IndexController indexController = new IndexController(gson, indexService);
        MetadataController metadataController = new MetadataController(gson, metadataStore);

        ActiveMqIndexer mqIndexer = new ActiveMqIndexer(
                gson,
                indexService,
                brokerUrl,
                queueName,
                reindexQueue,
                indexedQueue,
                hzNode
        );

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json");

        // /health + /index
        indexController.registerRoutes(app);
        metadataController.registerRoutes(app);

        // optional smoke endpoint
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

        app.events(ev -> ev.serverStopping(() -> {
            mqIndexer.close();
            hzProvider.shutdown();
        }));

        mqIndexer.start();
        app.start("0.0.0.0", port);
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
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));

        Path lakeRoot = Path.of("data_repository", "datalake_node1").normalize();
        Path indexRoot = Path.of("data_repository", "indexes").normalize();

        start(port, lakeRoot, indexRoot);
    }
}

/*
curl -i -X POST "http://localhost:7002/index" \
  -H "Content-Type: application/json" \
  -d '{ "lakePath": "20260112/23/1346.json" }'
 */
