# Hazelcast Global Configuration

This directory contains centralized Hazelcast configuration files for all services in the Stage 3 search engine.

## Configuration Files

### 1. `hazelcast-client.xml`
XML-based client configuration following Hazelcast 5.3 schema. This is the recommended format for declarative configuration.

**Usage:**
```java
ClientConfig config = new ClientConfig();
config.load(new FileInputStream("config/hazelcast-client.xml"));
HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
```

### 2. `hazelcast.properties`
Properties-based configuration for simple key-value settings. Easy to read and override via environment variables.

**Usage:**
```java
HazelcastProvider provider = HazelcastProvider.fromProperties("config/hazelcast.properties");
```

### 3. `HazelcastProvider.java`
Centralized Java provider class that can be copied to each service. Supports multiple configuration sources:

1. **Environment Variables** (highest priority):
   - `HZ_MEMBERS` - comma-separated list of Hazelcast member addresses
   - `HZ_CLUSTER` - cluster name
   - `NODE_ID` - instance name

2. **Properties File**:
   ```java
   HazelcastProvider.fromProperties("config/hazelcast.properties")
   ```

3. **XML Configuration**:
   ```java
   new HazelcastProvider("config/hazelcast-client.xml")
   ```

4. **Programmatic** (lowest priority):
   ```java
   new HazelcastProvider(members, cluster, instance)
   ```

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `hazelcast.cluster.name` | `stage3` | Name of the Hazelcast cluster |
| `hazelcast.members` | `localhost:5701` | Comma-separated member addresses |
| `hazelcast.connection.timeout` | `5000` | Connection timeout in milliseconds |
| `hazelcast.cluster.connect.timeout` | `10000` | Cluster connection timeout |
| `hazelcast.smart.routing` | `true` | Enable smart routing for better performance |
| `hazelcast.inverted.index.name` | `inverted-index` | Name of inverted index MultiMap |
| `hazelcast.claim.store.name` | `claim-store` | Name of claim store Map |
| `hazelcast.indexed.store.name` | `indexed-store` | Name of indexed books Set |
| `hazelcast.backup.count` | `2` | Number of synchronous backups |

## Using in Services

### Indexing Service
```java
String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "");
String hzCluster = System.getenv().getOrDefault("HZ_CLUSTER", "stage3");
String hzNode = System.getenv().getOrDefault("NODE_ID", "indexer-" + port);

HazelcastProvider provider = new HazelcastProvider(hzMembers, hzCluster, hzNode);
HazelcastInstance hz = provider.instance();
```

### Search Service
```java
String hzMembers = System.getenv().getOrDefault("HZ_MEMBERS", "localhost:5701");
HazelcastProvider provider = new HazelcastProvider(hzMembers, "stage3", "search-service");
HazelcastInstance hz = provider.instance();
```

### Docker Compose
```yaml
environment:
  - HZ_MEMBERS=hazelcast1:5701,hazelcast2:5701,hazelcast3:5701
  - HZ_CLUSTER=stage3
  - NODE_ID=indexer-1
```

## Best Practices

1. **Use environment variables** for deployment-specific configuration (member addresses, ports)
2. **Use XML/properties files** for static configuration (timeouts, backup counts)
3. **Copy `HazelcastProvider.java`** to each service's package and adjust as needed
4. **Always set a cluster name** to avoid accidental cluster merging
5. **Enable statistics** for monitoring in production

## Data Structures

All services use these standardized Hazelcast data structures:

- **`inverted-index`** (MultiMap): Term → List of document IDs
- **`claim-store`** (Map): Document ID → Indexer instance claim
- **`indexed-store`** (Set): Set of indexed document IDs

## Messaging Events (ActiveMQ)

The system uses JSON events with an `eventType` field:

- `document_ingested` (crawler → broker → indexer)
- `document_indexed` (indexer → broker)
- `reindex_request` (control → broker → indexer)

Queue names are configurable via environment variables:
`ACTIVEMQ_QUEUE` (ingest), `ACTIVEMQ_REINDEX_QUEUE` (reindex), `ACTIVEMQ_INDEXED_QUEUE` (indexed).

## Caching and Eviction

The inverted index is designed to be memory-resident, so eviction/expiration is intentionally disabled on the server side (`eviction-policy=NONE` in `hazelcast.xml` for maps). MultiMap does not expose eviction settings; the cluster is expected to be sized to hold the full index in memory.

Search clients can enable a **Near Cache** for read-heavy workloads. See `config/hazelcast-client.xml` for a sample Near Cache config for `inverted-index` (invalidate-on-change, binary format).

## Migration Guide

To migrate an existing service to use the global config:

1. Copy `HazelcastProvider.java` to your service's package
2. Update imports and package declaration
3. Replace custom Hazelcast initialization with `HazelcastProvider`
4. Set environment variables in deployment config
5. Test connection to Hazelcast cluster
