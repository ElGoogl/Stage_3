package com.shared.hazelcast;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Centralized Hazelcast client provider for all services.
 * Supports configuration via:
 * 1. Environment variables (highest priority)
 * 2. hazelcast.properties file
 * 3. hazelcast-client.xml file
 * 4. Programmatic defaults (lowest priority)
 */
public final class HazelcastProvider {

    private final HazelcastInstance hz;

    /**
     * Create Hazelcast client using environment variables and defaults
     */
    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        ClientConfig cfg = buildConfig(membersCsv, clusterName, instanceName);
        this.hz = HazelcastClient.newHazelcastClient(cfg);
    }

    /**
     * Create Hazelcast client using XML configuration file
     */
    public HazelcastProvider(String configFilePath) {
        ClientConfig cfg;
        try {
            cfg = new ClientConfig();
            cfg.load(new FileInputStream(configFilePath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Hazelcast config: " + configFilePath, e);
        }
        this.hz = HazelcastClient.newHazelcastClient(cfg);
    }

    /**
     * Create Hazelcast client using properties file
     */
    public static HazelcastProvider fromProperties(String propertiesPath) {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(propertiesPath)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties: " + propertiesPath, e);
        }

        String members = props.getProperty("hazelcast.members", "localhost:5701");
        String cluster = props.getProperty("hazelcast.cluster.name", "stage3");
        String instance = props.getProperty("hazelcast.instance.name", "default-client");

        return new HazelcastProvider(members, cluster, instance);
    }

    /**
     * Build client configuration programmatically
     */
    private ClientConfig buildConfig(String membersCsv, String clusterName, String instanceName) {
        ClientConfig cfg = new ClientConfig();

        // Set cluster name
        if (clusterName != null && !clusterName.isBlank()) {
            cfg.setClusterName(clusterName);
        } else {
            cfg.setClusterName("stage3");
        }

        // Set instance name
        if (instanceName != null && !instanceName.isBlank()) {
            cfg.setInstanceName(instanceName);
        }

        // Configure member addresses
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
            .setInitialBackoffMillis(1000)
            .setMaxBackoffMillis(30000)
            .setMultiplier(1.5)
            .setClusterConnectTimeoutMillis(10000);

        // Network settings
        cfg.getNetworkConfig()
            .setSmartRouting(true)
            .setConnectionTimeout(5000);

        cfg.getNetworkConfig()
            .getSocketOptions()
            .setTcpNoDelay(true)
            .setKeepAlive(true);

        // Enable statistics
        cfg.getMetricsConfig().setEnabled(true);
        cfg.setProperty("hazelcast.client.statistics.enabled", "true");
        cfg.setProperty("hazelcast.logging.type", "slf4j");

        return cfg;
    }

    public HazelcastInstance instance() {
        return hz;
    }

    public void shutdown() {
        try {
            if (hz != null) {
                hz.shutdown();
            }
        } catch (Exception ignored) {
        }
    }
}
