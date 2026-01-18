package com.indexer.hz;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.indexer.index.DocumentMetadataStore;

import java.util.Arrays;

public final class HazelcastProvider {

    private final HazelcastInstance hz;

    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        this.hz = startMemberInstance(membersCsv, clusterName, instanceName);
        System.out.println("[HZ] Started Hazelcast member instance");
    }

    private HazelcastInstance startMemberInstance(String membersCsv, String clusterName, String instanceName) {
        Config config = new Config();
        if (clusterName != null && !clusterName.isBlank()) {
            config.setClusterName(clusterName);
        }
        if (instanceName != null && !instanceName.isBlank()) {
            config.setInstanceName(instanceName);
        }

        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        if (membersCsv != null && !membersCsv.isBlank()) {
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            Arrays.stream(membersCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .forEach(m -> {
                        String address = m.contains(":") ? m : m + ":5701";
                        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(address);
                    });
        } else {
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
            config.getNetworkConfig().setPort(5701).setPortAutoIncrement(true).setPortCount(100);
            config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        }

        config.getMapConfig(DocumentMetadataStore.MAP_NAME)
                .setBackupCount(2)
                .setAsyncBackupCount(1);
        if (countMembers(membersCsv) >= 3) {
            config.getCPSubsystemConfig().setCPMemberCount(3);
        }

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

    private int countMembers(String membersCsv) {
        if (membersCsv == null || membersCsv.isBlank()) {
            return 0;
        }
        return (int) Arrays.stream(membersCsv.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .count();
    }
}
