package com.example.crawler;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

final class HazelcastClientFactory {
    private HazelcastClientFactory() {
    }

    static HazelcastInstance tryCreate(String hzMembersCsv, String clusterName) {
        if (hzMembersCsv == null || hzMembersCsv.isBlank()) {
            System.out.println("[HZ] HZ_MEMBERS not set -> Hazelcast de-dup disabled (NOT Stage-3 complete).");
            return null;
        }

        try {
            ClientConfig cfg = new ClientConfig();
            if (clusterName != null && !clusterName.isBlank()) {
                cfg.setClusterName(clusterName);
            }
            for (String addr : hzMembersCsv.split(",")) {
                String a = addr.trim();
                if (!a.isEmpty()) {
                    cfg.getNetworkConfig().addAddress(a);
                }
            }
            cfg.getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(5000);
            HazelcastInstance hz = HazelcastClient.newHazelcastClient(cfg);
            System.out.println("[HZ] Connected to Hazelcast cluster: " + hzMembersCsv);
            return hz;
        } catch (Exception e) {
            System.out.println("[HZ] Failed to connect to Hazelcast (" + e.getMessage() + "). De-dup disabled.");
            return null;
        }
    }
}
