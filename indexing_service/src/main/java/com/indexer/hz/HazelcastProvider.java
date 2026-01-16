package com.indexer.hz;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class HazelcastProvider {

    private final HazelcastInstance hz;

    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        boolean localMember = Boolean.parseBoolean(System.getenv().getOrDefault("LOCAL_HZ", "false"));

        if (localMember) {
            Config cfg = new Config();
            if (clusterName != null && !clusterName.isBlank()) {
                cfg.setClusterName(clusterName);
            }
            if (instanceName != null && !instanceName.isBlank()) {
                cfg.setInstanceName(instanceName);
            }
            this.hz = Hazelcast.newHazelcastInstance(cfg);
            return;
        }

        ClientConfig cfg = new ClientConfig();
        if (clusterName != null && !clusterName.isBlank()) {
            cfg.setClusterName(clusterName);
        }
        if (instanceName != null && !instanceName.isBlank()) {
            cfg.setInstanceName(instanceName);
        }

        List<String> members = parseMembers(membersCsv);
        if (members.isEmpty()) {
            members = List.of("localhost:5701");
        }

        cfg.getNetworkConfig().addAddress(members.toArray(new String[0]));
        cfg.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(10_000);

        this.hz = HazelcastClient.newHazelcastClient(cfg);
    }

    private static List<String> parseMembers(String membersCsv) {
        if (membersCsv == null || membersCsv.isBlank()) return List.of();
        return Arrays.stream(membersCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(m -> m.contains(":") ? m : (m + ":5701"))
                .collect(Collectors.toList());
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