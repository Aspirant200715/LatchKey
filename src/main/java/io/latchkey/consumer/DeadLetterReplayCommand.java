package io.latchkey.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Authenticated operator command that republishes a stored dead-letter record. */
@Component
@Profile("cli")
public class DeadLetterReplayCommand implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayCommand.class);
  private static final TypeReference<Map<String, List<String>>> HEADER_TYPE =
      new TypeReference<>() {};

  private final JdbcTemplate jdbc;
  private final KafkaTemplate<Object, Object> kafka;
  private final ObjectMapper mapper;
  private final String adminKey;
  private final String replayKey;

  public DeadLetterReplayCommand(
      JdbcTemplate jdbc,
      KafkaTemplate<Object, Object> kafka,
      ObjectMapper mapper,
      @Value("${latchkey.admin-key:}") String adminKey,
      @Value("${latchkey.replay-key:}") String replayKey) {
    this.jdbc = jdbc;
    this.kafka = kafka;
    this.mapper = mapper;
    this.adminKey = adminKey;
    this.replayKey = replayKey;
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    List<String> command = args.getNonOptionArgs();
    if (command.size() != 1 || !"replay".equals(command.get(0))) {
      throw new IllegalArgumentException("usage: replay --dlq-id=N [--dry-run]");
    }
    authenticate();

    String idOption = first(args.getOptionValues("dlq-id"));
    long dlqId;
    try {
      dlqId = Long.parseLong(idOption);
      if (dlqId < 1) throw new NumberFormatException();
    } catch (Exception e) {
      throw new IllegalArgumentException("--dlq-id must be a positive integer");
    }

    DeadLetter deadLetter = load(dlqId);
    if (deadLetter.headersJson() == null) {
      throw new IllegalStateException(
          "dead letter " + dlqId + " has no saved Kafka headers and cannot be replayed safely");
    }
    List<Header> headers = decodeHeaders(deadLetter.headersJson());
    boolean dryRun = args.containsOption("dry-run");
    if (dryRun) {
      log.info(
          "admin_replay action=dry_run dlq_id={} event_id={} topic={} partition={} offset={}",
          dlqId,
          deadLetter.eventId(),
          deadLetter.topic(),
          deadLetter.partition(),
          deadLetter.offset());
      System.out.printf(
          "Would replay DLQ %d to topic %s (original partition %d, offset %d, event_id %s).%n",
          dlqId,
          deadLetter.topic(),
          deadLetter.partition(),
          deadLetter.offset(),
          deadLetter.eventId());
      return;
    }

    ProducerRecord<Object, Object> record =
        new ProducerRecord<Object, Object>(
            deadLetter.topic(), null, null, deadLetter.recordKey(), deadLetter.rawMessage(), headers);
    kafka.send(record).get(30, TimeUnit.SECONDS);
    int updated =
        jdbc.update(
            "UPDATE dead_letters SET replayed_at = now() WHERE id = ? AND replayed_at IS NULL",
            dlqId);
    if (updated == 0) {
      // The event was already published by an overlapping operator invocation. Consumer inbox
      // deduplication makes this safe, and the audit line records the repeated operator action.
      log.warn("admin_replay action=already_marked dlq_id={} event_id={}", dlqId, deadLetter.eventId());
    } else {
      log.info(
          "admin_replay action=published dlq_id={} event_id={} topic={}",
          dlqId,
          deadLetter.eventId(),
          deadLetter.topic());
    }
    System.out.printf("Replayed DLQ %d to topic %s (event_id %s).%n", dlqId, deadLetter.topic(), deadLetter.eventId());
  }

  private void authenticate() {
    if (adminKey.isBlank()
        || replayKey.isBlank()
        || !MessageDigest.isEqual(
            adminKey.getBytes(StandardCharsets.UTF_8), replayKey.getBytes(StandardCharsets.UTF_8))) {
      throw new SecurityException("admin authentication failed");
    }
  }

  private DeadLetter load(long id) {
    return jdbc.query(
        "SELECT event_id, topic, partition, \"offset\", raw_message, record_key, headers::text"
            + " FROM dead_letters WHERE id = ?",
        rs -> {
          if (!rs.next()) throw new IllegalArgumentException("dead letter not found: " + id);
          UUID eventId = rs.getObject("event_id", UUID.class);
          return new DeadLetter(
              eventId,
              rs.getString("topic"),
              rs.getInt("partition"),
              rs.getLong("offset"),
              rs.getBytes("raw_message"),
              rs.getString("record_key"),
              rs.getString("headers"));
        },
        id);
  }

  private List<Header> decodeHeaders(String json) throws Exception {
    Map<String, List<String>> encoded = mapper.readValue(json, HEADER_TYPE);
    List<Header> headers = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : encoded.entrySet()) {
      for (String value : entry.getValue()) {
        headers.add(new RecordHeader(entry.getKey(), Base64.getDecoder().decode(value)));
      }
    }
    return headers;
  }

  private static String first(List<String> values) {
    return values == null || values.isEmpty() ? null : values.get(0);
  }

  private record DeadLetter(
      UUID eventId,
      String topic,
      int partition,
      long offset,
      byte[] rawMessage,
      String recordKey,
      String headersJson) {}
}
