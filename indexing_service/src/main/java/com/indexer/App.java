package com.indexer;

public final class App {
    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "7002"));
        java.nio.file.Path lakeRoot = java.nio.file.Path.of("data_repository", "datalake_node1").normalize();
        java.nio.file.Path indexRoot = java.nio.file.Path.of("data_repository", "indexes").normalize();

        String brokerUrl = System.getenv().getOrDefault("ACTIVEMQ_URL", "tcp://localhost:61616");
        String queueName = System.getenv().getOrDefault("ACTIVEMQ_QUEUE", "books.ingested");
        String reindexQueue = System.getenv().getOrDefault("ACTIVEMQ_REINDEX_QUEUE", "books.reindex");
        String indexedQueue = System.getenv().getOrDefault("ACTIVEMQ_INDEXED_QUEUE", "books.indexed");
        String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
        String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "stage3");
        String hzNode = System.getenv().getOrDefault("NODE_ID", "indexer-" + port);

        IndexingApplication app = new IndexingApplication(
                port,
                lakeRoot,
                indexRoot,
                brokerUrl,
                queueName,
                reindexQueue,
                indexedQueue,
                hzMembers,
                hzCluster,
                hzNode
        );
        app.start();
    }
}
