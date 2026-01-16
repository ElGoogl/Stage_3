package com.indexer.hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;

public final class HazelcastProvider {

    private final HazelcastInstance hz;
    private final boolean embeddedMode;

    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        // Try to connect to external cluster first, fall back to embedded if unavailable
        HazelcastInstance instance = null;
        boolean embedded = false;

        if (membersCsv != null && !membersCsv.isBlank()) {
            // Try to connect to external cluster
            try {
                System.out.println("[HZ] Attempting to connect to external Hazelcast cluster: " + membersCsv);
                instance = connectToCluster(membersCsv, clusterName, instanceName);
                System.out.println("[HZ] Successfully connected to external Hazelcast cluster");
            } catch (Exception e) {
                System.out.println("[HZ] Failed to connect to external cluster: " + e.getMessage());
                System.out.println("[HZ] Falling back to embedded Hazelcast instance");
            }
        }

        // If no external cluster available, start embedded instance
        if (instance == null) {
            instance = startEmbeddedInstance(clusterName, instanceName);
            embedded = true;
            System.out.println("[HZ] Started embedded Hazelcast instance (standalone mode)");
        }

        this.hz = instance;
        this.embeddedMode = embedded;
    }

    private HazelcastInstance connectToCluster(String membersCsv, String clusterName, String instanceName) {
        ClientConfig cfg = new ClientConfig();
        if (clusterName != null && !clusterName.isBlank()) {
            cfg.setClusterName(clusterName);
        }
        if (instanceName != null && !instanceName.isBlank()) {
            cfg.setInstanceName(instanceName);
        }

        // Configure connection to Hazelcast cluster members
        Arrays.stream(membersCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .forEach(m -> {
                    // Add :5701 if no port specified
                    String address = m.contains(":") ? m : m + ":5701";
                    cfg.getNetworkConfig().addAddress(address);
                });

        // Shorter timeout for quick failover to embedded mode
        cfg.getConnectionStrategyConfig()
            .getConnectionRetryConfig()
            .setClusterConnectTimeoutMillis(5000);

        return HazelcastClient.newHazelcastClient(cfg);
    }

    private HazelcastInstance startEmbeddedInstance(String clusterName, String instanceName) {
        Config config = new Config();
        if (clusterName != null && !clusterName.isBlank()) {
            config.setClusterName(clusterName);
        }
        if (instanceName != null && !instanceName.isBlank()) {
            config.setInstanceName(instanceName);
        }

        // Disable multicast and TCP-IP join for standalone mode
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        return Hazelcast.newHazelcastInstance(config);
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