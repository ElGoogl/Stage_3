package com.example.crawler;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

final class IngestionConfig {
    final Path dataDir;
    final String brokerUrl;
    final String queueName;
    final int port;
    final int replicationFactor;
    final List<String> replicaPeers;
    final String nodeId;
    final String hzMembers;
    final String hzCluster;

    private IngestionConfig(
            Path dataDir,
            String brokerUrl,
            String queueName,
            int port,
            int replicationFactor,
            List<String> replicaPeers,
            String nodeId,
            String hzMembers,
            String hzCluster
    ) {
        this.dataDir = dataDir;
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
        this.port = port;
        this.replicationFactor = replicationFactor;
        this.replicaPeers = replicaPeers;
        this.nodeId = nodeId;
        this.hzMembers = hzMembers;
        this.hzCluster = hzCluster;
    }

    static IngestionConfig fromEnv() {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7001"));
        Path dataDir = Path.of(System.getProperty("dataRepo", "data_repository/datalake_v1"));
        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String queueName = System.getenv().getOrDefault("ACTIVEMQ_QUEUE", "books.ingested");
        int replicationFactor = Integer.parseInt(System.getenv().getOrDefault("REPLICATION_FACTOR", "2"));
        List<String> replicaPeers = parseCommaList(System.getenv().getOrDefault("REPLICA_PEERS", ""));
        String nodeId = System.getenv().getOrDefault("NODE_ID", "crawler-" + port);
        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
        String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "");
        return new IngestionConfig(
                dataDir,
                brokerUrl,
                queueName,
                port,
                replicationFactor,
                replicaPeers,
                nodeId,
                hzMembers,
                hzCluster
        );
    }

    private static List<String> parseCommaList(String s) {
        if (s == null || s.isBlank()) {
            return List.of();
        }
        return Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(x -> !x.isBlank())
                .toList();
    }
}
