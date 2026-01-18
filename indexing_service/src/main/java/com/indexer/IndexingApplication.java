package com.indexer;

import com.google.gson.Gson;
import com.indexer.core.BookParser;
import com.indexer.core.IndexService;
import com.indexer.core.PathResolver;
import com.indexer.core.Tokenizer;
import com.indexer.hz.HazelcastProvider;
import com.indexer.index.ClaimStore;
import com.indexer.index.DocumentMetadataStore;
import com.indexer.index.IndexedStore;
import com.indexer.index.InvertedIndexStore;
import com.indexer.messaging.ActiveMqIndexer;
import com.indexer.web.IndexController;
import com.indexer.web.MetadataController;
import io.javalin.Javalin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class IndexingApplication {
    private final int port;
    private final Path lakeRoot;
    private final Path indexRoot;
    private final String brokerUrl;
    private final String queueName;
    private final String reindexQueue;
    private final String indexedQueue;
    private final String hzMembers;
    private final String hzCluster;
    private final String hzNode;

    IndexingApplication(int port,
                        Path lakeRoot,
                        Path indexRoot,
                        String brokerUrl,
                        String queueName,
                        String reindexQueue,
                        String indexedQueue,
                        String hzMembers,
                        String hzCluster,
                        String hzNode) {
        this.port = port;
        this.lakeRoot = lakeRoot;
        this.indexRoot = indexRoot;
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
        this.reindexQueue = reindexQueue;
        this.indexedQueue = indexedQueue;
        this.hzMembers = hzMembers;
        this.hzCluster = hzCluster;
        this.hzNode = hzNode;
    }

    Javalin start() {
        ensureDirExists(indexRoot);

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
        indexController.registerRoutes(app);
        metadataController.registerRoutes(app);

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

        app.start("0.0.0.0", port);
        mqIndexer.start();
        return app;
    }

    private static void ensureDirExists(Path dir) {
        if (Files.exists(dir)) {
            return;
        }
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create dir: " + dir, e);
        }
    }
}
