package com.example.crawler;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

public class ActiveMqPublisher {
    private final String brokerUrl;
    private final String queueName;

    public ActiveMqPublisher(String brokerUrl, String queueName) {
        this.brokerUrl = brokerUrl;
        this.queueName = queueName;
    }

    public void publish(String jsonPayload) {
        ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);

        try (Connection connection = factory.createConnection()) {
            connection.start();

            try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
                Destination destination = session.createQueue(queueName);
                MessageProducer producer = session.createProducer(destination);

                TextMessage msg = session.createTextMessage(jsonPayload);
                producer.send(msg);
            }
        } catch (JMSException e) {
            throw new RuntimeException("Failed to publish message to ActiveMQ", e);
        }
    }
}
