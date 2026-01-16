package com.indexer.hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

public final class HazelcastProvider {

    private final HazelcastInstance hz;

    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        ClientConfig cfg = new ClientConfig();
        if (clusterName != null && !clusterName.isBlank()) {
            cfg.setClusterName(clusterName);
        }
        if (instanceName != null && !instanceName.isBlank()) {
            cfg.setInstanceName(instanceName);
        }

        // Configure connection to Hazelcast cluster members
        if (membersCsv != null && !membersCsv.isBlank()) {
            Arrays.stream(membersCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .forEach(m -> {
                        // Add :5701 if no port specified
                        String address = m.contains(":") ? m : m + ":5701";
                        cfg.getNetworkConfig().addAddress(address);
                    });
        } else {
            // Default to localhost if no members specified
            cfg.getNetworkConfig().addAddress("localhost:5701");
        }

        // Connection retry settings
        cfg.getConnectionStrategyConfig()
            .getConnectionRetryConfig()
            .setClusterConnectTimeoutMillis(10000);

        this.hz = HazelcastClient.newHazelcastClient(cfg);
    }

    public HazelcastInstance instance() {
        return hz;
    }

    public void shutdown() {
        try {
            hz.shutdown();
        } catch (Exception ignored) {
        }
    }
}