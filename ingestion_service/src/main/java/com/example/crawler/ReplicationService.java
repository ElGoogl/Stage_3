package com.example.crawler;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class ReplicationService {
    private final HttpClient http;

    ReplicationService(HttpClient http) {
        this.http = http;
    }

    List<String> replicate(Path savedPath, String date, String hour, int bookId,
                           int replicationFactor, List<String> replicaPeers, String nodeId)
            throws IOException, InterruptedException {
        int neededRemoteReplicas = Math.max(0, replicationFactor - 1);
        List<String> replicaNodes = new ArrayList<>();
        replicaNodes.add(nodeId);

        if (neededRemoteReplicas == 0) {
            return replicaNodes;
        }

        String fileJson = Files.readString(savedPath, StandardCharsets.UTF_8);
        int success = 0;
        for (String peerBase : replicaPeers) {
            if (success >= neededRemoteReplicas) {
                break;
            }
            String url = peerBase.replaceAll("/+$", "") + "/replica/" + date + "/" + hour + "/" + bookId;
            if (postReplica(url, fileJson)) {
                success++;
                replicaNodes.add(peerBase);
            }
        }

        if (success < neededRemoteReplicas) {
            throw new ReplicationException(success, replicationFactor, replicaPeers);
        }

        return replicaNodes;
    }

    private boolean postReplica(String url, String jsonBody) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(jsonBody, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() >= 200 && resp.statusCode() < 300;
        } catch (Exception e) {
            return false;
        }
    }

    static final class ReplicationException extends RuntimeException {
        final int replicatedTotal;
        final int neededTotal;
        final List<String> replicaPeers;

        ReplicationException(int success, int replicationFactor, List<String> replicaPeers) {
            super("Replication failed");
            this.replicatedTotal = 1 + success;
            this.neededTotal = replicationFactor;
            this.replicaPeers = replicaPeers;
        }
    }
}
