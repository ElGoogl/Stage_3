package com.bd.search;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

final class HazelcastClientProvider {
    HazelcastInstance connect(String host, int port, String clusterName) {
        try {
            ClientConfig clientConfig = buildClientConfig(host, port, clusterName);
            clientConfig.getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(5000);
            return HazelcastClient.newHazelcastClient(clientConfig);
        } catch (Exception e) {
            return startEmbedded();
        }
    }

    private ClientConfig buildClientConfig(String host, int port, String clusterName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().addAddress(host + ":" + port);

        NearCacheConfig nearCache = new NearCacheConfig("inverted-index");
        nearCache.setInvalidateOnChange(true);
        nearCache.setInMemoryFormat(InMemoryFormat.BINARY);
        nearCache.setCacheLocalEntries(true);
        nearCache.setEvictionConfig(new EvictionConfig()
                .setEvictionPolicy(EvictionPolicy.NONE)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(0));
        clientConfig.addNearCacheConfig(nearCache);
        return clientConfig;
    }

    private HazelcastInstance startEmbedded() {
        Config config = new Config();
        config.setClusterName("search-cluster-embedded");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        return Hazelcast.newHazelcastInstance(config);
    }
}
