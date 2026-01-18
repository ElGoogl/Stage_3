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

Each service has its own Hazelcast setup. See the service source for exact configuration:
- `indexing_service/src/main/java/com/indexer/hz/HazelcastProvider.java`
- `search_service/src/main/java/com/bd/search/HazelcastClientProvider.java`
- `ingestion_service/src/main/java/com/example/crawler/HazelcastClientFactory.java`

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

1. Update the service-specific Hazelcast client/member setup to read `config/hazelcast-client.xml` or `config/hazelcast.properties`
2. Replace hardcoded members with `HZ_MEMBERS` and cluster name with `HZ_CLUSTER`
3. Test connection to the Hazelcast cluster
