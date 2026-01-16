package com.indexer.hz;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.indexer.index.InvertedIndexStore;

import java.util.Arrays;

public final class HazelcastProvider {

    private final HazelcastInstance hz;

    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        Config cfg = new Config();
        if (clusterName != null && !clusterName.isBlank()) cfg.setClusterName(clusterName);
        if (instanceName != null && !instanceName.isBlank()) cfg.setInstanceName(instanceName);

        MultiMapConfig mm = new MultiMapConfig(InvertedIndexStore.MAP_NAME);
        mm.setValueCollectionType(com.hazelcast.config.MultiMapConfig.ValueCollectionType.SET);
        mm.setBackupCount(1);
        mm.setAsyncBackupCount(0);
        cfg.addMultiMapConfig(mm);

        NetworkConfig net = cfg.getNetworkConfig();
        JoinConfig join = net.getJoin();

        if (membersCsv != null && !membersCsv.isBlank()) {
            join.getMulticastConfig().setEnabled(false);
            join.getTcpIpConfig().setEnabled(true);

            Arrays.stream(membersCsv.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isBlank())
                    .forEach(m -> join.getTcpIpConfig().addMember(m));
        } else {
            join.getTcpIpConfig().setEnabled(false);
            join.getMulticastConfig().setEnabled(true);
        }

        this.hz = Hazelcast.newHazelcastInstance(cfg);
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