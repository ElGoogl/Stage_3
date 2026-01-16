package com.indexer.mq;

import com.google.gson.Gson;
import com.indexer.core.IndexService;
import com.indexer.dto.IndexResponse;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public final class ActiveMqIngestConsumer implements AutoCloseable {

    private final Gson gson;
    private final IndexService indexService;

    private final String brokerUrl;
    private final String queueName;

    private Connection connection;
    private Session session;
    private MessageConsumer consumer;

    public ActiveMqIngestConsumer(Gson gson, IndexService indexService, String brokerUrl, String queueName) {
        this.gson = gson;
        this.indexService = indexService;
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void start() {
        try {
            ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
            connection = factory.createConnection();
            connection.start();

            // CLIENT_ACKNOWLEDGE: wir bestaetigen erst, wenn indexieren geklappt hat
            session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            Destination destination = session.createQueue(queueName);

            consumer = session.createConsumer(destination);
            consumer.setMessageListener(this::onMessage);

            System.out.println("[MQ] Listening on " + queueName + " at " + brokerUrl);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start ActiveMQ consumer", e);
        }
    }

    private void onMessage(Message message) {
        try {
            if (!(message instanceof TextMessage tm)) {
                // unbekanntes format, ack damit es nicht ewig wiederkommt
                message.acknowledge();
                return;
            }

            String json = tm.getText();
            IngestEvent evt = gson.fromJson(json, IngestEvent.class);

            if (evt == null || evt.lakePath == null || evt.lakePath.isBlank()) {
                message.acknowledge();
                return;
            }

            IndexResponse resp = indexService.index(evt.lakePath);

            boolean ok =
                    "ok".equals(resp.status()) ||
                            "already_indexed".equals(resp.status());

            if (ok) {
                message.acknowledge();
                System.out.println("[MQ] indexed bookId=" + resp.bookId() + " status=" + resp.status());
            } else {
                // nicht ack, dann kommt die Message spaeter nochmal
                System.out.println("[MQ] indexing failed status=" + resp.status() + " error=" + resp.error());
            }

        } catch (Exception e) {
            // nicht ack, damit retry moeglich bleibt
            System.out.println("[MQ] listener error: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        try { if (consumer != null) consumer.close(); } catch (Exception ignored) {}
        try { if (session != null) session.close(); } catch (Exception ignored) {}
        try { if (connection != null) connection.close(); } catch (Exception ignored) {}
    }

    private static final class IngestEvent {
        Integer bookId;
        String lakePath;
        String ingestedAt;
    }
}