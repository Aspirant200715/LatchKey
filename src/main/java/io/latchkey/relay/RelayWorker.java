package io.latchkey.relay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.latchkey.fault.FailPointException;
import io.latchkey.fault.FailPoints;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

/**
 * Publishes outbox rows to the broker, at least once.
 *
 * <p>Loop: claim a batch (lease) -> publish with acks=all -> mark published_at only after the
 * broker acknowledged. A crash anywhere leaves the lease to expire and the row is published again;
 * the duplicate is absorbed by the consumer inbox. Per-aggregate order inside a partition comes
 * from key = aggregate_id plus aggregate_version on every event.
 */
@Component
@Profile("relay")
public class RelayWorker implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(RelayWorker.class);

  private final OutboxRepository outbox;
  private final KafkaTemplate<String, String> kafka;
  private final KafkaAdmin admin;
  private final FailPoints failPoints;
  private final ObjectMapper mapper;
  private final Backoff backoff = new Backoff(1.0, 60.0, new Random());

  private final String topic;
  private final int partitions;
  private final int batchSize;
  private final long pollMs;
  private final double leaseSeconds;
  private final long publishTimeoutMs;
  private final long shutdownWaitMs;
  private final boolean enabled;
  private final String workerId =
      "relay-" + UUID.randomUUID().toString().substring(0, 8);

  private final CountDownLatch stopSignal = new CountDownLatch(1);
  private volatile boolean running;
  private volatile boolean topicReady;
  private Thread thread;

  public RelayWorker(
      OutboxRepository outbox,
      KafkaTemplate<String, String> kafka,
      KafkaAdmin admin,
      FailPoints failPoints,
      ObjectMapper mapper,
      @Value("${latchkey.relay.topic:order.created}") String topic,
      @Value("${latchkey.relay.partitions:6}") int partitions,
      @Value("${latchkey.relay.batch-size:100}") int batchSize,
      @Value("${latchkey.relay.poll-ms:200}") long pollMs,
      @Value("${latchkey.relay.lease-seconds:30}") double leaseSeconds,
      @Value("${latchkey.relay.publish-timeout-ms:10000}") long publishTimeoutMs,
      @Value("${latchkey.relay.shutdown-wait-ms:20000}") long shutdownWaitMs,
      @Value("${latchkey.relay.enabled:true}") boolean enabled) {
    this.outbox = outbox;
    this.kafka = kafka;
    this.admin = admin;
    this.failPoints = failPoints;
    this.mapper = mapper;
    this.topic = topic;
    this.partitions = partitions;
    this.batchSize = batchSize;
    this.pollMs = pollMs;
    this.leaseSeconds = leaseSeconds;
    this.publishTimeoutMs = publishTimeoutMs;
    this.shutdownWaitMs = shutdownWaitMs;
    this.enabled = enabled;
  }

  @Override
  public void start() {
    running = true;
    thread = new Thread(this::loop, "outbox-relay");
    thread.start();
    log.info("relay {} started, topic={} batch={} lease={}s", workerId, topic, batchSize, leaseSeconds);
  }

  /** Stop claiming, let the in-flight batch finish, then release any leases still held. */
  @Override
  public void stop() {
    running = false;
    stopSignal.countDown();
    if (thread != null) {
      try {
        thread.join(shutdownWaitMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (thread.isAlive()) {
        log.warn("relay {} did not finish within {} ms; leases will expire", workerId, shutdownWaitMs);
      }
    }
  }

  @Override
  public boolean isAutoStartup() {
    return enabled; // false only in tests that exercise the repository without a live worker
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE; // start last, stop first: before the Kafka producer closes
  }

  private void loop() {
    try {
      while (running) {
        try {
          ensureTopic();
          List<OutboxRow> rows = outbox.claimBatch(batchSize, workerId, leaseSeconds);
          if (rows.isEmpty()) {
            sleep(pollMs);
            continue;
          }
          publishBatch(rows);
        } catch (Exception e) {
          // DB or broker trouble: never die, never spin. Rows keep their state and are retried.
          log.warn("relay loop error: {}", e.getClass().getSimpleName());
          sleep(1000);
        }
      }
    } finally {
      try {
        int released = outbox.releaseLeases(workerId);
        log.info("relay {} stopped, released {} leases", workerId, released);
      } catch (Exception e) {
        log.warn("relay {} could not release leases: {}", workerId, e.getClass().getSimpleName());
      }
    }
  }

  private void ensureTopic() {
    if (!topicReady) {
      admin.createOrModifyTopics(TopicBuilder.name(topic).partitions(partitions).replicas(1).build());
      topicReady = true;
    }
  }

  private void publishBatch(List<OutboxRow> rows) {
    record Pending(OutboxRow row, CompletableFuture<SendResult<String, String>> result) {}
    List<Pending> pending = new ArrayList<>(rows.size());
    for (OutboxRow row : rows) {
      try {
        pending.add(new Pending(row, kafka.send(toRecord(row))));
      } catch (Exception e) {
        fail(row, e);
      }
    }
    for (Pending p : pending) {
      try {
        p.result().get(publishTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException e) {
        fail(p.row(), e.getCause() != null ? e.getCause() : e);
        continue;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return; // leases expire; rows are retried
      }
      // Broker acknowledged. Only now may published_at be set.
      try {
        failPoints.hit(FailPoints.AFTER_PUBLISH_BEFORE_MARK);
        outbox.markPublished(p.row().eventId());
      } catch (FailPointException e) {
        log.warn("simulated crash after publish for event {}", p.row().eventId());
      } catch (DataAccessException e) {
        log.warn("could not mark event {} published: lease will expire and it will be re-sent",
            p.row().eventId());
      }
    }
  }

  private void fail(OutboxRow row, Throwable cause) {
    String message = cause.getClass().getSimpleName() + ": " + cause.getMessage();
    try {
      outbox.markFailed(row.eventId(), message, backoff.delaySeconds(row.attempts()));
    } catch (DataAccessException e) {
      log.warn("could not record failure for event {}", row.eventId());
    }
    log.warn("publish failed for event {} (attempt {}): {}", row.eventId(), row.attempts(),
        cause.getClass().getSimpleName());
  }

  private ProducerRecord<String, String> toRecord(OutboxRow row) throws java.io.IOException {
    ProducerRecord<String, String> record =
        new ProducerRecord<>(topic, null, row.partitionKey(), row.payload());
    // event_id is the row's id, generated in the producing transaction: it never changes on retry.
    header(record, "event_id", row.eventId().toString());
    header(record, "tenant_id", row.tenantId().toString());
    header(record, "aggregate_version", String.valueOf(row.aggregateVersion()));
    header(record, "schema_version", String.valueOf(row.schemaVersion()));
    header(record, "event_type", row.eventType());
    JsonNode extra = mapper.readTree(row.headers());
    for (Iterator<Map.Entry<String, JsonNode>> it = extra.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      header(record, e.getKey(), e.getValue().asText());
    }
    return record;
  }

  private static void header(ProducerRecord<String, String> record, String name, String value) {
    record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
  }

  private void sleep(long ms) {
    try {
      stopSignal.await(ms, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
