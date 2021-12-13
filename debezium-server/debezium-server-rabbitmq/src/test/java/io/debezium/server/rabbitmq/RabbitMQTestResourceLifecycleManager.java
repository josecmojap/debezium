package io.debezium.server.rabbitmq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.GenericContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class RabbitMQTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    public static final int RABBITMQ_PORT = 5672;
    public static final String RABBITMQ_IMAGE = "rabbitmq:3-alpine";

    private static final AtomicBoolean running = new AtomicBoolean(false);
    private static final GenericContainer<?> container = new GenericContainer<>(RABBITMQ_IMAGE)
            .withExposedPorts(RABBITMQ_PORT);

    private static synchronized void start(boolean ignored) {
        if (!running.get()) {
            container.start();
            running.set(true);
        }
    }

    public static String getRabbitMQContainerHost() {
        start(true);
        return container.getContainerIpAddress();
    }

    public static Integer getRabbitMQContainerPort() {
        if (container.isRunning()) {
            return container.getFirstMappedPort();
        }
        return null;
    }

    @Override
    public Map<String, String> start() {
        start(true);
        Map<String, String> params = new ConcurrentHashMap<>();
        return params;
    }

    @Override
    public void stop() {
        try {
            container.stop();
        }
        catch (Exception e) {
            // ignored
        }
        running.set(false);
    }
}
