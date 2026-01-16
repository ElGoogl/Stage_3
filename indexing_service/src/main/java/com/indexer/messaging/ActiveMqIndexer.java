package com.indexer.messaging;

import com.google.gson.Gson;
import com.indexer.core.IndexService;
import com.indexer.dto.IndexResponse;
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
import java.util.Map;

public final class ActiveMqIndexer implements AutoCloseable {

    private static final Duration RECONNECT_DELAY = Duration.ofSeconds(3);

    private final Gson gson;
    private final IndexService indexService;
    private final String brokerUrl;
    private final String queueName;

    private volatile boolean running;
    private Thread worker;

    public ActiveMqIndexer(Gson gson, IndexService indexService, String brokerUrl, String queueName) {
        this.gson = gson;
        this.indexService = indexService;
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(this::runLoop, "activemq-indexer");
        worker.setDaemon(true);
        worker.start();
    }

    private void runLoop() {
        while (running) {
            try {
                listenOnce();
            } catch (Exception e) {
                System.out.println("[INDEXER] ActiveMQ listener error: " + e.getMessage());
                sleepQuietly(RECONNECT_DELAY);
            }
        }
    }

    private void listenOnce() throws JMSException {
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
                        handleMessage(msg);
                    }
                }
            }
        }
    }

    private void handleMessage(Message msg) {
        if (!(msg instanceof TextMessage textMessage)) {
            System.out.println("[INDEXER] Ignoring non-text message: " + msg);
            return;
        }

        try {
            String payload = textMessage.getText();
            Map<?, ?> event = gson.fromJson(payload, Map.class);
            Object lakePath = event != null ? event.get("lakePath") : null;

            if (lakePath == null) {
                System.out.println("[INDEXER] Missing lakePath in message: " + payload);
                return;
            }

            IndexResponse response = indexService.index(lakePath.toString());
            System.out.println("[INDEXER] Indexed from queue: " + response.status()
                    + " bookId=" + response.bookId()
                    + " lakePath=" + response.lakePath());
        } catch (Exception e) {
            System.out.println("[INDEXER] Failed to process message: " + e.getMessage());
        }
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
        if (worker != null) {
            worker.interrupt();
        }
    }
}
