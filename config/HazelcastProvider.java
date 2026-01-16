package com.shared.hazelcast;
















































































































































```[HZ] HZ_MEMBERS not set -> Hazelcast de-dup disabled (NOT Stage-3 complete).```**No Hazelcast (Ingestion Service):**```[HZ] Started embedded Hazelcast instance (standalone mode)[HZ] Falling back to embedded Hazelcast instance[HZ] Failed to connect to external cluster: <error>```**Standalone/Embedded Mode:**```[HZ] Successfully connected to external Hazelcast cluster[HZ] Attempting to connect to external Hazelcast cluster: hazelcast1:5701```**External Cluster Mode:**Services log their Hazelcast connection mode on startup:## Detecting Mode- **Performance Testing**: Use distributed mode to properly benchmark cluster performance- **Testing**: Standalone mode is sufficient for functional testing of individual services- **Production**: Use Docker Compose with external Hazelcast cluster for full distributed capabilities- **Development**: Use standalone mode for quick testing and development## Recommendations   - No horizontal scaling4. **No Load Balancing**: Single instance handles all requests   - Service restart loses in-memory data3. **No High Availability**: No automatic failover or replication   - Same book might be downloaded multiple times   - Multiple crawler instances won't coordinate2. **No Distributed De-duplication**: Ingestion service uses local memory for de-dup   - You must use the same service instance for indexing and searching   - Indexing service's inverted index is not shared with search service1. **No Data Sharing**: Each service has its own isolated Hazelcast instanceWhen services run in standalone/embedded mode (without external Hazelcast cluster):## Limitations in Standalone Mode- `REPLICATION_FACTOR`: Number of replicas (default: `2`)- `ACTIVEMQ_URL`: ActiveMQ broker URL (default: `tcp://localhost:61616`)- `PORT`: Service port (default: `7001`)- `NODE_ID`: Instance identifier (default: `crawler-{PORT}`)- `HZ_MEMBERS`: Comma-separated list of Hazelcast members (optional)### Ingestion Service- `PORT`: Service port (default: `8080`)- `HAZELCAST_PORT`: Hazelcast port (default: `5701`)- `HAZELCAST_HOST`: Hazelcast host (default: `localhost`)### Search Service- `PORT`: Service port (default: `7002`)- `NODE_ID`: Instance identifier (default: `indexer-{PORT}`)- `HZ_CLUSTER`: Cluster name (default: `stage3`)- `HZ_MEMBERS`: Comma-separated list of Hazelcast members (e.g., `hazelcast1:5701,hazelcast2:5701`)### Indexing Service## Environment VariablesDe-duplication will use local memory instead of Hazelcast.```java -jar target/ingestion_service-1.0-SNAPSHOT.jarmvn clean packagecd ingestion_service```bash#### Ingestion ServiceThe service will try to connect to `localhost:5701` by default, then fall back to embedded mode.```java -jar target/search_service-1.0-SNAPSHOT.jarmvn clean packagecd search_service```bash#### Search Service```PORT=7002 java -jar target/indexing_service-1.0-SNAPSHOT.jar```bashOr with environment variables:```java -jar target/indexing_service-1.0-SNAPSHOT.jarmvn clean package# No HZ_MEMBERS set - will use embedded Hazelcastcd indexing_service```bash#### Indexing Service### Without Docker (Standalone Mode)Services will connect to the external Hazelcast cluster nodes.```docker-compose up```bashStart the full cluster:### With Docker (Distributed Mode)## Running Services  - Automatically appends `-embedded` to cluster name in standalone mode  - Shorter connection timeouts for faster failover  - New method `isEmbeddedMode()` to check if running in standalone mode  - Enhanced with embedded mode support- **Changes**:- **File**: `config/HazelcastProvider.java`### 4. **Shared Configuration** (`config`)  - Logs appropriate warnings about de-dup being disabled  - Uses `LocalClaimFallback` for de-duplication when Hazelcast is unavailable  - Returns `null` if `HZ_MEMBERS` is not set or connection fails- **Status**: Already handles missing Hazelcast gracefully- **File**: `src/main/java/com/example/crawler/App.java`### 3. **Ingestion Service** (`ingestion_service`)  - Uses cluster name `search-cluster-embedded` in standalone mode  - Falls back to embedded Hazelcast instance in standalone mode if connection fails  - Tries to connect to external Hazelcast at `HAZELCAST_HOST:HAZELCAST_PORT`- **Changes**:- **File**: `src/main/java/com/bd/search/SearchApp.java`### 2. **Search Service** (`search_service`)  - Shorter connection timeout (5 seconds) for quick failover  - Falls back to embedded Hazelcast instance if connection fails or `HZ_MEMBERS` is not set  - Attempts to connect to external Hazelcast cluster specified by `HZ_MEMBERS` environment variable- **Changes**:- **File**: `src/main/java/com/indexer/hz/HazelcastProvider.java`### 1. **Indexing Service** (`indexing_service`)## Changes MadeAll services in Stage 3 can now run without Docker. They automatically detect whether an external Hazelcast cluster is available and fall back to embedded mode if not.import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
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
 * 
 * Automatically falls back to embedded Hazelcast instance if external cluster unavailable.
 */
public final class HazelcastProvider {

    private final HazelcastInstance hz;
    private final boolean embeddedMode;

    /**
     * Create Hazelcast client using environment variables and defaults
     * Falls back to embedded instance if external cluster is unavailable
     */
    public HazelcastProvider(String membersCsv, String clusterName, String instanceName) {
        HazelcastInstance instance = null;
        boolean embedded = false;

        // Try to connect to external cluster first if members specified
        if (membersCsv != null && !membersCsv.isBlank()) {
            try {
                System.out.println("[HZ] Attempting to connect to external Hazelcast cluster: " + membersCsv);
                instance = connectToCluster(membersCsv, clusterName, instanceName);
                System.out.println("[HZ] Successfully connected to external Hazelcast cluster");
            } catch (Exception e) {
                System.out.println("[HZ] Failed to connect to external cluster: " + e.getMessage());
                System.out.println("[HZ] Falling back to embedded Hazelcast instance");
            }
        }

        // Fall back to embedded instance if no external cluster available
        if (instance == null) {
            instance = startEmbeddedInstance(clusterName, instanceName);
            embedded = true;
            System.out.println("[HZ] Started embedded Hazelcast instance (standalone mode)");
        }

        this.hz = instance;
        this.embeddedMode = embedded;
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
        this.embeddedMode = false;
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
     * Connect to external Hazelcast cluster
     */
    private HazelcastInstance connectToCluster(String membersCsv, String clusterName, String instanceName) {
        ClientConfig cfg = buildClientConfig(membersCsv, clusterName, instanceName);
        return HazelcastClient.newHazelcastClient(cfg);
    }

    /**
     * Start embedded Hazelcast instance for standalone mode
     */
    private HazelcastInstance startEmbeddedInstance(String clusterName, String instanceName) {
        Config config = new Config();
        
        if (clusterName != null && !clusterName.isBlank()) {
            config.setClusterName(clusterName + "-embedded");
        } else {
            config.setClusterName("stage3-embedded");
        }
        
        if (instanceName != null && !instanceName.isBlank()) {
            config.setInstanceName(instanceName);
        }

        // Disable multicast and TCP-IP join for standalone mode
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        return Hazelcast.newHazelcastInstance(config);
    }

    /**
     * Build client configuration programmatically
     */
    private ClientConfig buildClientConfig(String membersCsv, String clusterName, String instanceName) {
    /**
     * Build client configuration programmatically
     */
    private ClientConfig buildClientConfig(String membersCsv, String clusterName, String instanceName) {
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
            .setInitialBackoffMillis(1000)
            .setMaxBackoffMillis(5000)
            .setMultiplier(1.5)
            .setClusterConnectTimeoutMillis(5000);
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
        retboolean isEmbeddedMode() {
        return embeddedMode;
    }

    public urn hz;
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
