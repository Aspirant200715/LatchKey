package io.latchkey.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Boots the real consumer against PostgreSQL and Redpanda with an isolated inventory database. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("consumer")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class ConsumerTestBase {

  protected static final String TOPIC = "order.created.consumer-tests";
  private static final String GROUP_ID = "latchkey-consumer-tests-" + UUID.randomUUID();

  static {
    createInventoryDatabase();
    createTopic();
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", ConsumerTestBase::inventoryJdbcUrl);
    registry.add("spring.datasource.username", Containers.POSTGRES::getUsername);
    registry.add("spring.datasource.password", Containers.POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", () -> Containers.redpanda().getBootstrapServers());
    registry.add("server.port", () -> "0");
    registry.add("management.server.port", () -> "0");
    registry.add("latchkey.consumer.group-id", () -> GROUP_ID);
    registry.add("latchkey.consumer.topic", () -> TOPIC);
    registry.add("latchkey.inventory.seed.enabled", () -> "false");
  }

  @Autowired protected JdbcTemplate jdbc;

  protected void seedInventory(String sku, int stock) {
    jdbc.update("INSERT INTO inventory (sku, stock) VALUES (?, ?)", sku, stock);
  }

  protected void publish(UUID eventId, String sku, int quantity, int schemaVersion) {
    String payload =
        "{\"order_id\":\"" + UUID.randomUUID() + "\",\"tenant_id\":\""
            + UUID.randomUUID() + "\",\"sku\":\"" + sku + "\",\"quantity\":" + quantity + "}";
    publishRaw(eventId, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8), schemaVersion);
  }

  protected void publishRaw(UUID eventId, byte[] payload, int schemaVersion) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.redpanda().getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties)) {
      ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, UUID.randomUUID().toString(), payload);
      record.headers().add("event_id", eventId.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      record.headers().add("event_type", "order.created".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      record.headers().add("schema_version", Integer.toString(schemaVersion).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      producer.send(record).get(Duration.ofSeconds(10).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      throw new IllegalStateException("could not publish consumer test event", e);
    }
  }

  private static void createInventoryDatabase() {
    String url = Containers.POSTGRES.getJdbcUrl();
    try (Connection connection =
            DriverManager.getConnection(
                url, Containers.POSTGRES.getUsername(), Containers.POSTGRES.getPassword());
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE inventory_db");
    } catch (SQLException e) {
      if (!"42P04".equals(e.getSQLState())) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }

  private static String inventoryJdbcUrl() {
    String url = Containers.POSTGRES.getJdbcUrl();
    int databaseSegment = url.lastIndexOf("/test");
    if (databaseSegment < 0) {
      throw new IllegalStateException("unexpected Testcontainers JDBC URL: " + url);
    }
    return url.substring(0, databaseSegment) + "/inventory_db" + url.substring(databaseSegment + 5);
  }

  private static void createTopic() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", Containers.redpanda().getBootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {
      try {
        admin.createTopics(java.util.List.of(new NewTopic(TOPIC, 6, (short) 1)))
            .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
      } catch (java.util.concurrent.ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw e;
        }
      }
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }
}
