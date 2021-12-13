package io.debezium.server.rabbitmq;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.enterprise.event.Observes;

import org.awaitility.Awaitility;
import org.fest.assertions.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;

import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.server.events.ConnectorStartedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(RabbitMQTestResourceLifecycleManager.class)
public class RabbitMQStreamIT {

    private static final int MESSAGE_COUNT = 4;
    private static final String STREAM_NAME = "testc.inventory.customers";

    {
        Testing.Files.delete(RabbitMQTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(RabbitMQTestConfigSource.OFFSET_STORE_PATH);
    }

    private static ConnectionFactory factory;
    private static Connection connection;

    void setupDependencies(@Observes ConnectorStartedEvent event) throws IOException, TimeoutException {
        Testing.Print.enable();

        factory.setHost(RabbitMQTestResourceLifecycleManager.getRabbitMQContainerHost());
        factory.setPort(RabbitMQTestResourceLifecycleManager.getRabbitMQContainerPort());
        factory.setUsername("guest");
        factory.setPassword("guest");

        connection = factory.newConnection();

    }

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @AfterAll
    static void afterAll() throws IOException {
        connection.close();
    }

    @Test
    public void testRabbbitMQStream() throws Exception {
        Testing.Print.enable();
        final List<Delivery> messages = new ArrayList<>();
        Channel channel = connection.createChannel();
        channel.basicQos(100); // QoS must be specified
        Awaitility.await().atMost(Duration.ofSeconds(RabbitMQTestConfigSource.waitForSeconds())).until(() -> {
            channel.basicConsume(
                    "cdc-stream",
                    false,
                    Collections.singletonMap("x-stream-offset", "first"), // "first" offset specification
                    (consumerTag, message) -> {
                        messages.add(message);
                        channel.basicAck(message.getEnvelope().getDeliveryTag(), false); // ack is required
                    },
                    consumerTag -> {
                        Testing.print(String.format("%s: %s", "Consumer tag", consumerTag));
                    });
            return messages.size() >= MESSAGE_COUNT;
        });
        Assertions.assertThat(messages.size() >= MESSAGE_COUNT);
        connection.close();
    }
}
