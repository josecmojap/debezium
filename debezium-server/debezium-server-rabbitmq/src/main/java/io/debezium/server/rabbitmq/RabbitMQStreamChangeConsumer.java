package io.debezium.server.rabbitmq;

import java.util.*;

import javax.annotation.PostConstruct;
import javax.enterprise.context.Dependent;
import javax.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

@Named("rabbitmq")
@Dependent
public class RabbitMQStreamChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQStreamChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.rabbitmq.";
    private static final String PROP_HOST = PROP_PREFIX + "host";
    private static final String PROP_PORT = PROP_PREFIX + "port";
    private static final String PROP_USER = PROP_PREFIX + "user";
    private static final String PROP_PASSWORD = PROP_PREFIX + "password";
    private static final String PROP_EXCHANGE_NAME = PROP_PREFIX + "exchange.name";
    private static final String PROP_STREAM_QUEUE_NAME = PROP_PREFIX + "stream.name";

    private Map<String,String> bindings = new HashMap<String,String>();

    private Optional<String> host;
    private Optional<Integer> port;
    private Optional<String> user;
    private Optional<String> password;
    private Optional<String> exchangeName;
    private Optional<String> queueName;

    private Connection connection;
    private Channel channel;

    private boolean streamLike = true;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    @ConfigProperty(name = PROP_PREFIX + "null.value", defaultValue = "default")
    String nullValue;

    public boolean isStreamLike() {
        return streamLike;
    }

    @PostConstruct
    void connect() {
        this.LOGGER.info("Load configuration...");
        ConnectionFactory factory = new ConnectionFactory();

        final Config config = ConfigProvider.getConfig();
        host = config.getOptionalValue(PROP_HOST, String.class);
        port = config.getOptionalValue(PROP_PORT, Integer.class);
        user = config.getOptionalValue(PROP_USER, String.class);
        password = config.getOptionalValue(PROP_PASSWORD, String.class);
        exchangeName = config.getOptionalValue(PROP_EXCHANGE_NAME, String.class);
        queueName = config.getOptionalValue(PROP_STREAM_QUEUE_NAME, String.class);

        factory.setHost(host.orElse("localhost"));
        factory.setPort(port.orElse(5672));
        factory.setUsername(user.orElse("guest"));
        factory.setPassword(password.orElse("guest"));

        try {
            this.LOGGER.info("Creating connection with rabbitmq...");
            this.connection = factory.newConnection();
            this.channel = this.connection.createChannel();
            if (exchangeName.isPresent()) {
                this.channel.exchangeDeclare(exchangeName.get(), "cdc");
                this.streamLike = false;
            }
            else {
                this.channel.queueDeclare(queueName.orElse("cdc_stream"), true, false, false, Collections.singletonMap("x-queue-type", "stream"));
            }

        }
        catch (Exception e) {
            this.LOGGER.error("Failed to connect", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        for (ChangeEvent<Object, Object> record : records) {
            LOGGER.trace("Received event '{}'", record);

            try {

                String routingKey = record.destination();
                String message = (record.value() != null) ? getString(record.value()) : nullValue;
                Map<String, Object> headers = new HashMap<String, Object>();
                headers.put("record_key", (record.key() != null) ? getString(record.key()) : nullKey);

                LOGGER.info("\n\t\tRouting key    :{}", routingKey);
                LOGGER.info("\n\t\tMessage        :{}", message);
                LOGGER.info("\n\t\tHeaders        :{}", headers);

                if (this.isStreamLike()) {
                    LOGGER.info("Post message to queue {} with routing key {}", queueName.orElse("cdc_stream"), routingKey);
                    this.channel.basicPublish(queueName.orElse("cdc_stream"), routingKey, new AMQP.BasicProperties().builder().headers(headers).build(),
                            message.getBytes("UTF-8"));
                }
                else {
                    if (!this.bindings.containsKey(routingKey)) {
                        String queueName = routingKey.replace(".", "_");
                        this.channel.queueDeclare(queueName, true, false, false, null);
                        this.channel.queueBind(queueName, this.exchangeName.get(), routingKey);
                        this.bindings.put(routingKey, queueName);
                        LOGGER.info("Queue {} is creating binding from exchange {} with routing key {}", queueName, exchangeName.get(), routingKey);
                    }
                    LOGGER.info("Post message to exchange {} with routing key {}", exchangeName.get(), routingKey);
                    this.channel.basicPublish(exchangeName.get(), routingKey, new AMQP.BasicProperties().builder().headers(headers).build(), message.getBytes("UTF-8"));
                }

            }
            catch (Exception e) {
                LOGGER.error("Error on register event on rabbitmq", e);
            }
        }

    }
}
