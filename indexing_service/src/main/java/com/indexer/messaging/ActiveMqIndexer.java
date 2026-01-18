package com.indexer.messaging;

import com.google.gson.Gson;
import com.indexer.core.IndexService;
import com.indexer.dto.IndexResponse;
import com.indexer.messaging.ActiveMqPublisher;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class ActiveMqIndexer implements AutoCloseable {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(3);
    private static final String EVENT_INGESTED = "document_ingested";
    private static final String EVENT_INDEXED = "document_indexed";
    private static final String EVENT_REINDEX = "reindex_request";

    private final Gson gson;
    private final IndexService indexService;
    private final String brokerUrl;
    private final String ingestQueue;
    private final String reindexQueue;
    private final String indexedQueue;
    private final String nodeId;
    private final ActiveMqPublisher indexedPublisher;

    private volatile boolean running;
    private Thread ingestWorker;
    private Thread reindexWorker;

    public ActiveMqIndexer(
            Gson gson,
            IndexService indexService,
            String brokerUrl,
            String ingestQueue,
            String reindexQueue,
            String indexedQueue,
            String nodeId
    ) {
        this.gson = gson;
        this.indexService = indexService;
        this.brokerUrl = brokerUrl;
        this.ingestQueue = ingestQueue;
        this.reindexQueue = reindexQueue;
        this.indexedQueue = indexedQueue;
        this.nodeId = nodeId;
        this.indexedPublisher = new ActiveMqPublisher(brokerUrl, indexedQueue);
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        ingestWorker = new Thread(() -> runLoop(ingestQueue, this::handleIngestMessage), "activemq-indexer-ingest");
        ingestWorker.setDaemon(true);
        ingestWorker.start();

        reindexWorker = new Thread(() -> runLoop(reindexQueue, this::handleReindexMessage), "activemq-indexer-reindex");
        reindexWorker.setDaemon(true);
        reindexWorker.start();
    }

    private void runLoop(String queueName, Consumer<Message> handler) {
        while (running) {
            try {
                listenOnce(queueName, handler);
            } catch (Exception e) {
                System.out.println("[INDEXER] ActiveMQ listener error (" + queueName + "): " + e.getMessage());
                sleepQuietly(RECONNECT_DELAY);
            }
        }
    }

    private void listenOnce(String queueName, Consumer<Message> handler) throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        try (Connection connection = factory.createConnection()) {
            connection.start();
            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Destination destination = session.createQueue(queueName);
                try (MessageConsumer consumer = session.createConsumer(destination)) {
                    System.out.println("[INDEXER] Listening for ActiveMQ messages on " + queueName);
                    while (running) {
                        Message msg = consumer.receive(1000);
                        if (msg == null) {
                            continue;
                        }
                        handler.accept(msg);
                    }
                }
            }
        }
    }

    private void handleIngestMessage(Message msg) {
        handleMessage(msg, false);
    }

    private void handleReindexMessage(Message msg) {
        handleMessage(msg, true);
    }

    private void handleMessage(Message msg, boolean forceReindex) {
        if (!(msg instanceof TextMessage textMessage)) {
            System.out.println("[INDEXER] Ignoring non-text message: " + msg);
            return;
        }

        try {
            String payload = textMessage.getText();
            Map<?, ?> event = gson.fromJson(payload, Map.class);
            Object eventTypeRaw = event != null ? event.get("eventType") : null;
            String eventType = eventTypeRaw != null ? eventTypeRaw.toString() : EVENT_INGESTED;
            Object lakePath = event != null ? event.get("lakePath") : null;

            if (EVENT_INDEXED.equals(eventType)) {
                return;
            }

            if (!EVENT_INGESTED.equals(eventType) && !EVENT_REINDEX.equals(eventType)) {
                System.out.println("[INDEXER] Unknown event type: " + eventType + " payload=" + payload);
                return;
            }

            if (lakePath == null) {
                System.out.println("[INDEXER] Missing lakePath in message: " + payload);
                return;
            }

            IndexResponse response = indexService.index(lakePath.toString(), forceReindex);
            System.out.println("[INDEXER] Indexed from queue: " + response.status()
                    + " bookId=" + response.bookId()
                    + " lakePath=" + response.lakePath());

            if ("ok".equals(response.status()) || "already_indexed".equals(response.status())) {
                publishIndexedEvent(response, forceReindex);
            }
        } catch (Exception e) {
            System.out.println("[INDEXER] Failed to process message: " + e.getMessage());
        }
    }

    private void publishIndexedEvent(IndexResponse response, boolean reindex) {
        Map<String, Object> event = new HashMap<>();
        event.put("eventType", EVENT_INDEXED);
        event.put("bookId", response.bookId());
        event.put("lakePath", response.lakePath());
        event.put("indexedAt", Instant.now().toString());
        event.put("status", response.status());
        event.put("reindex", reindex);
        event.put("nodeId", nodeId);
        indexedPublisher.publish(gson.toJson(event));
    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        running = false;
        if (ingestWorker != null) ingestWorker.interrupt();
        if (reindexWorker != null) reindexWorker.interrupt();
    }
}
