package com.indexer.index;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import com.indexer.dto.DocumentMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public final class DocumentMetadataStore {

    public static final String MAP_NAME = "doc-metadata";

    private final IMap<Integer, DocumentMetadata> map;
    private final CPSubsystem cp;
    private final ConcurrentMap<Integer, ReentrantLock> localLocks = new ConcurrentHashMap<>();
    private volatile boolean cpAvailable;

    public DocumentMetadataStore(HazelcastInstance hz) {
        this.map = hz.getMap(MAP_NAME);
        this.cp = hz.getCPSubsystem();
        int cpMembers = hz.getConfig().getCPSubsystemConfig().getCPMemberCount();
        this.cpAvailable = cpMembers >= 3;
    }

    public MetadataLock lockFor(int bookId) {
        if (cpAvailable) {
            try {
                FencedLock lock = cp.getLock("doc-metadata-lock-" + bookId);
                return new CpMetadataLock(lock);
            } catch (Exception e) {
                cpAvailable = false;
            }
        }
        ReentrantLock lock = localLocks.computeIfAbsent(bookId, id -> new ReentrantLock());
        return new LocalMetadataLock(lock);
    }

    public DocumentMetadata get(int bookId) {
        return map.get(bookId);
    }

    public void put(int bookId, DocumentMetadata metadata) {
        map.set(bookId, metadata);
    }

    public List<DocumentMetadata> list(int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = limit <= 0 ? 100 : limit;

        List<DocumentMetadata> items = new ArrayList<>(safeLimit);
        int idx = 0;
        for (DocumentMetadata md : map.values()) {
            if (idx++ < safeOffset) {
                continue;
            }
            items.add(md);
            if (items.size() >= safeLimit) {
                break;
            }
        }
        return items;
    }

    private static final class CpMetadataLock implements MetadataLock {
        private final FencedLock lock;

        private CpMetadataLock(FencedLock lock) {
            this.lock = lock;
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }

    private static final class LocalMetadataLock implements MetadataLock {
        private final ReentrantLock lock;

        private LocalMetadataLock(ReentrantLock lock) {
            this.lock = lock;
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }
}
