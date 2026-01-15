package com.example.crawler;

import com.google.gson.Gson;
import io.javalin.Javalin;

import java.io.IOException;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

// Hazelcast (for de-dup / coordination)
// If you don't have Hazelcast on classpath yet, add it to this module and import these:
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.ISet;

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

    // Base datalake dir. Override with: -DdataRepo=...
    private static final Path DATA_DIR = Paths.get(
            System.getProperty("dataRepo", "data_repository/datalake_v1")
    );

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

        // --- Runtime config ---
        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String queueName = System.getenv().getOrDefault("ACTIVEMQ_QUEUE", "books.ingested");
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7001"));

        int replicationFactor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "2"));
        List<String> replicaPeers = parseCommaList(System.getenv().getOrDefault("REPLICA_PEERS", ""));
        String nodeId = System.getenv().getOrDefault("NODE_ID", "crawler-" + port);

        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
        HazelcastInstance hz = tryCreateHazelcastClient(hzMembers);
        ISet<Integer> claimedBooks = (hz != null) ? hz.getSet("claimed-books") : null;

        ActiveMqPublisher publisher = new ActiveMqPublisher(brokerUrl, queueName);
        HttpClient http = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        // Ensure datalake exists
        if (!Files.exists(DATA_DIR)) {
            try {
                Files.createDirectories(DATA_DIR);
                System.out.println("[INIT] Created datalake directory at: " + DATA_DIR.toAbsolutePath());
            } catch (IOException e) {
                throw new RuntimeException("Failed to create datalake directory: " + DATA_DIR, e);
            }
        }

        Javalin app = Javalin.create(cfg -> cfg.http.defaultContentType = "application/json")
                .start(port);

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

            Path folder = DATA_DIR.resolve(date).resolve(hour);
            Files.createDirectories(folder);
            Path outFile = folder.resolve(bookId + ".json");

            Files.writeString(outFile, jsonBody, StandardCharsets.UTF_8);

            ctx.result(gson.toJson(Map.of(
                    "status", "replicated",
                    "book_id", bookId,
                    "path", folder.toString().replace("\\", "/"),
                    "node", nodeId
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

            // 1) De-dup coordination (Hazelcast preferred)
            if (claimedBooks != null) {
                boolean acquired = claimedBooks.add(bookId);
                if (!acquired) {
                    ctx.status(409).result(gson.toJson(Map.of(
                            "book_id", bookId,
                            "status", "already_claimed"
                    )));
                    return;
                }
            } else {
                // Fallback (single-node only). Not enough for Stage 3, but avoids crashes if Hazelcast is down.
                if (!LocalClaimFallback.tryClaim(bookId)) {
                    ctx.status(409).result(gson.toJson(Map.of(
                            "book_id", bookId,
                            "status", "already_claimed_local_fallback"
                    )));
                    return;
                }
            }

            // 2) Download & Save (local)
            Optional<Path> savedPathOpt = Downloader.downloadAndSave(bookId, DATA_DIR);
            if (savedPathOpt.isEmpty()) {
                // release claim on failure so someone else can retry
                releaseClaim(claimedBooks, bookId);
                ctx.status(500).result(gson.toJson(Map.of("book_id", bookId, "status", "failed")));
                return;
            }

            Path savedPath = savedPathOpt.get();
            LocalDateTime now = LocalDateTime.now();

            // Determine date/hour from savedPath: DATA_DIR/date/hour/bookId.json
            String date;
            String hour;
            try {
                Path hourDir = savedPath.getParent();          // .../hour
                Path dateDir = hourDir.getParent();           // .../date
                hour = hourDir.getFileName().toString();
                date = dateDir.getFileName().toString();
            } catch (Exception ex) {
                releaseClaim(claimedBooks, bookId);
                ctx.status(500).result(gson.toJson(Map.of(
                        "book_id", bookId,
                        "status", "failed",
                        "error", "Could not parse date/hour folders from savedPath: " + savedPath
                )));
                return;
            }

            // 3) Replicate to peers (until replicationFactor reached)
            // local counts as 1
            int neededRemoteReplicas = Math.max(0, replicationFactor - 1);

            // We will store which nodes have the file. Local is always included.
            List<String> replicaNodes = new ArrayList<>();
            replicaNodes.add(nodeId);

            if (neededRemoteReplicas > 0) {
                String fileJson;
                try {
                    fileJson = Files.readString(savedPath, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    releaseClaim(claimedBooks, bookId);
                    ctx.status(500).result(gson.toJson(Map.of(
                            "book_id", bookId,
                            "status", "failed",
                            "error", "Failed reading stored file for replication"
                    )));
                    return;
                }

                int success = 0;

                // Try peers in order; stop once we have enough
                for (String peerBase : replicaPeers) {
                    if (success >= neededRemoteReplicas) break;

                    // POST http://peer:7001/replica/{date}/{hour}/{bookId}
                    String url = peerBase.replaceAll("/+$", "") + "/replica/" + date + "/" + hour + "/" + bookId;

                    if (postReplica(http, url, fileJson)) {
                        success++;
                        // store peer "id" as base url (or you can map to node IDs if you want)
                        replicaNodes.add(peerBase);
                    }
                }

                if (success < neededRemoteReplicas) {
                    // IMPORTANT: Do not publish to ActiveMQ if replication failed.
                    releaseClaim(claimedBooks, bookId);
                    ctx.status(502).result(gson.toJson(Map.of(
                            "book_id", bookId,
                            "status", "downloaded_but_replication_failed",
                            "replication_factor", replicationFactor,
                            "replicated_total", (1 + success),
                            "needed_total", replicationFactor,
                            "replica_peers", replicaPeers
                    )));
                    return;
                }
            }

            // 4) Publish event to ActiveMQ only after replication succeeded
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("bookId", bookId);
            event.put("lakePath", savedPath.toString().replace("\\", "/"));
            event.put("ingestedAt", now.toString());
            event.put("replicas", replicaNodes); // helpful for debug/report

            String payloadJson = gson.toJson(event);

            try {
                publisher.publish(payloadJson);
            } catch (RuntimeException ex) {
                // book is stored and replicated, but indexers won't be notified
                // You could keep claim or release claim; for retries, it's often OK to release.
                releaseClaim(claimedBooks, bookId);

                ctx.status(502).result(gson.toJson(Map.of(
                        "book_id", bookId,
                        "status", "downloaded_and_replicated_but_event_failed",
                        "path", savedPath.getParent().toString().replace("\\", "/"),
                        "file", savedPath.getFileName().toString(),
                        "replicas", replicaNodes,
                        "error", ex.getMessage()
                )));
                return;
            }

            // 5) Response
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "downloaded");
            resp.put("path", savedPath.getParent().toString().replace("\\", "/"));
            resp.put("file", savedPath.getFileName().toString());
            resp.put("replication_factor", replicationFactor);
            resp.put("replicas", replicaNodes);
            resp.put("event_sent", true);
            resp.put("queue", queueName);

            ctx.result(gson.toJson(resp));
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

            boolean exists = fileExistsRecursively(DATA_DIR, bookId + ".json");

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
                List<Integer> ids = listBookIdsRecursively(DATA_DIR);
                ctx.result(gson.toJson(Map.of("count", ids.size(), "books", ids)));
            } catch (IOException e) {
                ctx.status(500).result(gson.toJson(Map.of("error", "Failed to list datalake", "details", e.getMessage())));
            }
        });
    }

    // --
    // Helpers
    // --

    private static List<String> parseCommaList(String s) {
        if (s == null || s.isBlank()) return List.of();
        return Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(x -> !x.isBlank())
                .collect(Collectors.toList());
    }

    private static boolean postReplica(HttpClient http, String url, String jsonBody) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(java.time.Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() >= 200 && resp.statusCode() < 300;
        } catch (Exception e) {
            return false;
        }
    }

    private static HazelcastInstance tryCreateHazelcastClient(String hzMembersCsv) {
        if (hzMembersCsv == null || hzMembersCsv.isBlank()) {
            System.out.println("[HZ] HZ_MEMBERS not set -> Hazelcast de-dup disabled (NOT Stage-3 complete).");
            return null;
        }

        try {
            ClientConfig cfg = new ClientConfig();
            // Example: "hazelcast1:5701,hazelcast2:5701"
            for (String addr : hzMembersCsv.split(",")) {
                String a = addr.trim();
                if (!a.isEmpty()) cfg.getNetworkConfig().addAddress(a);
            }
            HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);
            System.out.println("[HZ] Connected to Hazelcast cluster: " + hzMembersCsv);
            return hz;
        } catch (Exception e) {
            System.out.println("[HZ] Failed to connect to Hazelcast (" + e.getMessage() + "). De-dup disabled.");
            return null;
        }
    }

    private static void releaseClaim(ISet<Integer> claimedBooks, int bookId) {
        try {
            if (claimedBooks != null) claimedBooks.remove(bookId);
        } catch (Exception ignored) {
        } finally {
            LocalClaimFallback.release(bookId);
        }
    }

    private static boolean fileExistsRecursively(Path root, String fileName) {
        try (var stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .anyMatch(p -> p.getFileName().toString().equals(fileName));
        } catch (IOException e) {
            return false;
        }
    }

    private static List<Integer> listBookIdsRecursively(Path root) throws IOException {
        try (var stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(n -> n.endsWith(".json"))
                    .map(n -> n.substring(0, n.length() - ".json".length()))
                    .flatMap(s -> {
                        try {
                            return java.util.stream.Stream.of(Integer.parseInt(s));
                        } catch (NumberFormatException e) {
                            return java.util.stream.Stream.empty();
                        }
                    })
                    .sorted()
                    .toList();
        }
    }

    /**
     * Only a fallback if Hazelcast isn't available.
     */
    private static final class LocalClaimFallback {
        private static final Set<Integer> claimed = ConcurrentHashMap.newKeySet();

        static boolean tryClaim(int bookId) {
            return claimed.add(bookId);
        }

        static void release(int bookId) {
            claimed.remove(bookId);
        }
    }
}
