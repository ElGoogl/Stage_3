package com.example.crawler;

import com.google.gson.Gson;
import com.hazelcast.collection.ISet;
import com.hazelcast.core.HazelcastInstance;
import io.javalin.Javalin;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;

/**
 * Ingestion service (Stage 3):
 * - Downloads a Gutenberg book
 * - Stores JSON into a datalake folder (yyyyMMdd/HH)
 * - Replicates the stored file to peer crawler nodes (replication factor R)
 * - Coordinates de-duplication across crawler instances (Hazelcast ISet)
 * - Publishes an "ingested" event to ActiveMQ only AFTER replication succeeded
 */
public class App {

    private static final Gson gson = new Gson();

    // ---- Config keys (env) ----
    // ActiveMQ
    //   ACTIVEMQ_URL=tcp://activemq:61616
    //   ACTIVEMQ_QUEUE=books.ingested
    // Replication
    //   REPLICA_PEERS=http://crawler2:7001,http://crawler3:7001
    //   REPLICATION_FACTOR=2   (counts local as 1; so R=2 -> replicate to 1 peer)
    // Coordination (Hazelcast)
    //   HZ_MEMBERS=hazelcast1:5701,hazelcast2:5701
    //   NODE_ID=crawler1  (for reporting/replica list)
    public static void main(String[] args) {

        IngestionConfig config = IngestionConfig.fromEnv();
        HazelcastInstance hz = HazelcastClientFactory.tryCreate(config.hzMembers, config.hzCluster);
        ISet<Integer> claimedBooks = (hz != null) ? hz.getSet("claimed-books-ingestion") : null;
        ClaimService claimService = (claimedBooks != null)
                ? new HazelcastClaimService(claimedBooks, new LocalClaimService())
                : new LocalClaimService();

        ActiveMqPublisher publisher = new ActiveMqPublisher(config.brokerUrl, config.queueName);
        HttpClient http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        DatalakeRepository repository = new DatalakeRepository(config.dataDir);
        try {
            repository.ensureBaseDir();
            System.out.println("[INIT] Created datalake directory at: " + config.dataDir.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create datalake directory: " + config.dataDir, e);
        }

        ReplicationService replicationService = new ReplicationService(http);
        IngestionService ingestionService = new IngestionService(
                gson,
                claimService,
                repository,
                replicationService,
                publisher,
                config.queueName,
                config.replicationFactor,
                config.replicaPeers,
                config.nodeId
        );

        Javalin app = Javalin.create(cfg -> {
            cfg.http.defaultContentType = "application/json";
            cfg.http.maxRequestSize = 10_000_000L;
        })
                .start("0.0.0.0", config.port);

        // --
        // Replication endpoint
        // Peers call this to push a replica of a stored file.
        //
        // URL format:
        //   POST /replica/{date}/{hour}/{book_id}
        // Body:
        //   raw JSON content of the book file (the same you store locally)
        // --
        app.post("/replica/{date}/{hour}/{book_id}", ctx -> {
            String date = ctx.pathParam("date");
            String hour = ctx.pathParam("hour");
            int bookId;
            try {
                bookId = Integer.parseInt(ctx.pathParam("book_id"));
            } catch (NumberFormatException e) {
                ctx.status(400).result(gson.toJson(Map.of("error", "book_id must be an integer")));
                return;
            }

            String jsonBody = ctx.body();
            if (jsonBody == null || jsonBody.isBlank()) {
                ctx.status(400).result(gson.toJson(Map.of("error", "empty body")));
                return;
            }

            repository.storeReplica(date, hour, bookId, jsonBody);

            ctx.result(gson.toJson(Map.of(
                    "status", "replicated",
                    "book_id", bookId,
                    "path", repository.baseDir().resolve(date).resolve(hour).toString().replace("\\", "/"),
                    "node", config.nodeId
            )));
        });

        // --
        // Ingest endpoint
        //  - De-dup/coordination via Hazelcast set "claimed-books"
        //  - Replication to peers (replicationFactor)
        //  - Publish to ActiveMQ only after replication succeeded
        // --
        app.post("/ingest/{book_id}", ctx -> {
            int bookId;
            try {
                bookId = Integer.parseInt(ctx.pathParam("book_id"));
            } catch (NumberFormatException e) {
                ctx.status(400).result(gson.toJson(Map.of("error", "book_id must be an integer")));
                return;
            }
            IngestionResult result = ingestionService.ingest(bookId);
            ctx.status(result.status).result(gson.toJson(result.payload));
        });

        // --
        // Status (recursive)
        // --
        app.get("/ingest/status/{book_id}", ctx -> {
            int bookId;
            try {
                bookId = Integer.parseInt(ctx.pathParam("book_id"));
            } catch (NumberFormatException e) {
                ctx.status(400).result(gson.toJson(Map.of("error", "book_id must be an integer")));
                return;
            }

            boolean exists = repository.fileExists(bookId + ".json");

            Map<String, Object> resp = Map.of(
                    "book_id", bookId,
                    "status", exists ? "available" : "missing"
            );
            ctx.result(gson.toJson(resp));
        });

        // --
        // List (recursive)
        // --
        app.get("/ingest/list", ctx -> {
            try {
                List<Integer> ids = repository.listBookIds();
                ctx.result(gson.toJson(Map.of("count", ids.size(), "books", ids)));
            } catch (IOException e) {
                ctx.status(500).result(gson.toJson(Map.of("error", "Failed to list datalake", "details", e.getMessage())));
            }
        });
    }
}
