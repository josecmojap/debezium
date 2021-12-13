package io.debezium.server.rabbitmq;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class RabbitMQTestConfigSource extends TestConfigSource {

    public RabbitMQTestConfigSource() {
        Map<String, String> rabbitMQTest = new HashMap<>();

        rabbitMQTest.put("debezium.sink.type", "rabbitmq");
        rabbitMQTest.put("debezium.sink.rabbitmq.host", RabbitMQTestResourceLifecycleManager.getRabbitMQContainerHost());
        rabbitMQTest.put("debezium.sink.rabbitmq.port", String.format("%d", RabbitMQTestResourceLifecycleManager.getRabbitMQContainerPort()));
        rabbitMQTest.put("debezium.sink.rabbitmq.user", "guest");
        rabbitMQTest.put("debezium.sink.rabbitmq.password", "guest");
        rabbitMQTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        rabbitMQTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                OFFSET_STORE_PATH.toAbsolutePath().toString());
        rabbitMQTest.put("debezium.source.offset.flush.interval.ms", "0");
        rabbitMQTest.put("debezium.source.database.server.name", "testc");
        rabbitMQTest.put("debezium.source.schema.include.list", "inventory");
        rabbitMQTest.put("debezium.source.table.include.list", "inventory.customers");

        config = rabbitMQTest;
    }
}
