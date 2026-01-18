package com.indexer.hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

public final class HazelcastClientProvider {

    private final HazelcastInstance client;

    public HazelcastClientProvider(String membersCsv, String clusterName) {
        this.client = connectToCluster(membersCsv, clusterName);
        System.out.println("[HZ] Connected to Hazelcast cluster as client");
    }

    private HazelcastInstance connectToCluster(String membersCsv, String clusterName) {
        ClientConfig clientConfig = new ClientConfig();
        
        if (clusterName != null && !clusterName.isBlank()) {
            clientConfig.setClusterName(clusterName);
        }
        
        if (membersCsv != null && !membersCsv.isBlank()) {
            Arrays.stream(membersCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .forEach(address -> {
                        String fullAddress = address.contains(":") ? address : address + ":5701";
                        clientConfig.getNetworkConfig().addAddress(fullAddress);
                    });
        }
        
        // Configure connection retry
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(30000)
                .setInitialBackoffMillis(1000)
                .setMaxBackoffMillis(10000);
        
        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    public HazelcastInstance instance() {
        return client;
    }

    public void shutdown() {
        try {
            client.shutdown();
        } catch (Exception e) {
            System.err.println("[HZ] Error shutting down client: " + e.getMessage());
        }
    }
}
