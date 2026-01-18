package com.example.crawler;

import com.google.gson.Gson;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class IngestionService {
    private final Gson gson;
    private final ClaimService claimService;
    private final DatalakeRepository repository;
    private final ReplicationService replicationService;
    private final ActiveMqPublisher publisher;
    private final String queueName;
    private final int replicationFactor;
    private final List<String> replicaPeers;
    private final String nodeId;

    IngestionService(Gson gson,
                     ClaimService claimService,
                     DatalakeRepository repository,
                     ReplicationService replicationService,
                     ActiveMqPublisher publisher,
                     String queueName,
                     int replicationFactor,
                     List<String> replicaPeers,
                     String nodeId) {
        this.gson = gson;
        this.claimService = claimService;
        this.repository = repository;
        this.replicationService = replicationService;
        this.publisher = publisher;
        this.queueName = queueName;
        this.replicationFactor = replicationFactor;
        this.replicaPeers = replicaPeers;
        this.nodeId = nodeId;
    }

    IngestionResult ingest(int bookId) {
        if (!claimService.tryClaim(bookId)) {
            Optional<Path> existing = repository.findFile(bookId + ".json");
            if (existing.isPresent()) {
                Path existingPath = existing.get();
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("book_id", bookId);
                resp.put("status", "downloaded");
                resp.put("path", existingPath.getParent().toString().replace("\\", "/"));
                resp.put("file", existingPath.getFileName().toString());
                resp.put("replication_factor", replicationFactor);
                resp.put("replicas", List.of(nodeId));
                resp.put("event_sent", false);
                resp.put("queue", queueName);
                resp.put("note", "already_claimed_using_existing_file");
                return new IngestionResult(200, resp);
            }
            return new IngestionResult(409, Map.of(
                    "book_id", bookId,
                    "status", "already_claimed"
            ));
        }

        Optional<Path> savedPathOpt = Downloader.downloadAndSave(bookId, repository.baseDir());
        if (savedPathOpt.isEmpty()) {
            claimService.release(bookId);
            return new IngestionResult(500, Map.of(
                    "book_id", bookId,
                    "status", "failed"
            ));
        }

        Path savedPath = savedPathOpt.get();
        LocalDateTime now = LocalDateTime.now();

        String date;
        String hour;
        try {
            Path hourDir = savedPath.getParent();
            Path dateDir = hourDir.getParent();
            hour = hourDir.getFileName().toString();
            date = dateDir.getFileName().toString();
        } catch (Exception ex) {
            claimService.release(bookId);
            return new IngestionResult(500, Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "error", "Could not parse date/hour folders from savedPath: " + savedPath
            ));
        }

        List<String> replicaNodes;
        try {
            replicaNodes = replicationService.replicate(savedPath, date, hour, bookId,
                    replicationFactor, replicaPeers, nodeId);
        } catch (ReplicationService.ReplicationException ex) {
            claimService.release(bookId);
            return new IngestionResult(502, Map.of(
                    "book_id", bookId,
                    "status", "downloaded_but_replication_failed",
                    "replication_factor", replicationFactor,
                    "replicated_total", ex.replicatedTotal,
                    "needed_total", ex.neededTotal,
                    "replica_peers", ex.replicaPeers
            ));
        } catch (Exception ex) {
            claimService.release(bookId);
            return new IngestionResult(500, Map.of(
                    "book_id", bookId,
                    "status", "failed",
                    "error", "Failed reading stored file for replication"
            ));
        }

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "document_ingested");
        event.put("bookId", bookId);
        event.put("lakePath", savedPath.toString().replace("\\", "/"));
        event.put("ingestedAt", now.toString());
        event.put("replicas", replicaNodes);

        String payloadJson = gson.toJson(event);

        try {
            publisher.publish(payloadJson);
        } catch (RuntimeException ex) {
            claimService.release(bookId);
            return new IngestionResult(502, Map.of(
                    "book_id", bookId,
                    "status", "downloaded_and_replicated_but_event_failed",
                    "path", savedPath.getParent().toString().replace("\\", "/"),
                    "file", savedPath.getFileName().toString(),
                    "replicas", replicaNodes,
                    "error", ex.getMessage()
            ));
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("book_id", bookId);
        resp.put("status", "downloaded");
        resp.put("path", savedPath.getParent().toString().replace("\\", "/"));
        resp.put("file", savedPath.getFileName().toString());
        resp.put("replication_factor", replicationFactor);
        resp.put("replicas", replicaNodes);
        resp.put("event_sent", true);
        resp.put("queue", queueName);

        return new IngestionResult(200, resp);
    }
}
