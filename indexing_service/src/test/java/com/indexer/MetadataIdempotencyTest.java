package com.indexer;

import com.google.gson.Gson;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.indexer.core.BookParser;
import com.indexer.core.IndexService;
import com.indexer.core.PathResolver;
import com.indexer.core.Tokenizer;
import com.indexer.dto.DocumentMetadata;
import com.indexer.dto.IndexResponse;
import com.indexer.index.ClaimStore;
import com.indexer.index.DocumentMetadataStore;
import com.indexer.index.IndexedStore;
import com.indexer.index.InvertedIndexStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class MetadataIdempotencyTest {

    private static HazelcastInstance hz;
    private static IndexService indexService;
    private static DocumentMetadataStore metadataStore;

    private static Path lakeRoot;
    private static Path indexRoot;

    @BeforeAll
    static void setup() {
        lakeRoot = Paths.get("..", "data_repository", "datalake_node1").normalize();
        indexRoot = Paths.get("..", "data_repository", "indexes").normalize();

        Config config = new Config();
        config.setClusterName("test-cluster");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.getCPSubsystemConfig().setCPMemberCount(1);

        hz = Hazelcast.newHazelcastInstance(config);

        InvertedIndexStore invertedIndex = new InvertedIndexStore(hz);
        ClaimStore claimStore = new ClaimStore(hz);
        IndexedStore indexedStore = new IndexedStore(hz);
        metadataStore = new DocumentMetadataStore(hz);

        Gson gson = new Gson();
        BookParser bookParser = new BookParser(gson);
        Tokenizer tokenizer = new Tokenizer();
        PathResolver resolver = new PathResolver(lakeRoot);

        indexService = new IndexService(
                resolver,
                indexRoot,
                claimStore,
                invertedIndex,
                indexedStore,
                metadataStore,
                "test-node",
                bookParser,
                tokenizer
        );
    }

    @AfterAll
    static void teardown() {
        if (hz != null) {
            hz.shutdown();
        }
    }

    @BeforeEach
    void resetState() throws Exception {
        hz.getMap(DocumentMetadataStore.MAP_NAME).clear();
        hz.getMap(IndexedStore.MAP_NAME).clear();
        hz.getMultiMap(InvertedIndexStore.MAP_NAME).clear();
        hz.getSet("claimed-books").clear();

        Files.deleteIfExists(indexRoot.resolve("1346.index.json"));
    }

    @Test
    void duplicateMessageWithSameHashDoesNotReindex() {
        IndexResponse first = indexService.index("20260112/23/1346.json");
        assertEquals("ok", first.status());

        IndexResponse second = indexService.index("20260112/23/1346.json");
        assertEquals("already_indexed", second.status());
    }

    @Test
    void duplicateMessageWithDifferentHashTriggersReindex() {
        metadataStore.put(1346, new DocumentMetadata(
                1346,
                "old-hash",
                Instant.now().toString(),
                0,
                "test-node",
                DocumentMetadata.Status.INDEXED
        ));

        IndexResponse resp = indexService.index("20260112/23/1346.json");
        assertEquals("ok", resp.status());

        DocumentMetadata updated = metadataStore.get(1346);
        assertNotEquals("old-hash", updated.contentHash());
        assertEquals(DocumentMetadata.Status.INDEXED, updated.status());
    }

    @Test
    void concurrentIndexingUsesMetadataLock() throws Exception {
        ExecutorService exec = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<String> statuses = Collections.synchronizedList(new ArrayList<>());

        Runnable task = () -> {
            try {
                start.await();
                statuses.add(indexService.index("20260112/23/1346.json").status());
            } catch (Exception ignored) {
            }
        };

        Future<?> f1 = exec.submit(task);
        Future<?> f2 = exec.submit(task);
        start.countDown();

        f1.get();
        f2.get();
        exec.shutdownNow();

        assertEquals(2, statuses.size());
        assertTrue(statuses.stream().allMatch(s -> s.equals("ok") || s.equals("already_indexed")));
        assertTrue(statuses.contains("already_indexed"));
    }
}
