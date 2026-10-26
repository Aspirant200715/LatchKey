package io.latchkey.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/** Applies an order event and records its deduplication marker atomically in inventory_db. */
@Service
@Profile("consumer")
public class InventoryEventProcessor {

  private static final String CONSUMER_NAME = "inventory-reservations-v1";

  private final JdbcTemplate jdbc;
  private final TransactionTemplate transaction;
  private final ObjectMapper mapper;

  public InventoryEventProcessor(
      JdbcTemplate jdbc, TransactionTemplate transaction, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.transaction = transaction;
    this.mapper = mapper;
  }

  public Outcome process(ConsumerRecord<String, byte[]> record) {
    byte[] raw = record.value() == null ? new byte[0] : record.value();
    UUID eventId = uuidHeader(record, "event_id");
    int schemaVersion = intHeader(record, "schema_version", -1);
    String eventType = stringHeader(record, "event_type");
    byte[] payloadHash = sha256(raw);

    return transaction.execute(
        status -> {
          if (eventId == null) {
            return deadLetter(record, null, "missing or invalid event_id header", raw);
          }

          byte[] previousHash = existingHash(eventId);
          if (previousHash != null) {
            return duplicateOrMismatch(record, eventId, raw, payloadHash, previousHash);
          }

          Event event;
          try {
            event = parse(decodeUtf8(raw), eventType, schemaVersion);
          } catch (InvalidEventException e) {
            // Do not mark poison messages as processed: an operator may repair and replay them
            // with the original event_id. A crash before offset ack can create another DLQ row.
            deadLetter(record, eventId, e.getMessage(), raw);
            return Outcome.DEAD_LETTERED;
          }

          List<Boolean> inserted =
              jdbc.query(
                  "INSERT INTO consumer_inbox (consumer_name, event_id, payload_hash)"
                      + " VALUES (?, ?, ?) ON CONFLICT (consumer_name, event_id) DO NOTHING"
                      + " RETURNING true",
                  (rs, row) -> rs.getBoolean(1),
                  CONSUMER_NAME,
                  eventId,
                  payloadHash);
          if (inserted.isEmpty()) {
            previousHash = existingHash(eventId);
            return duplicateOrMismatch(record, eventId, raw, payloadHash, previousHash);
          }

          int changed =
              jdbc.update(
                  "UPDATE inventory SET stock = stock - ? WHERE sku = ? AND stock >= ?",
                  event.quantity(),
                  event.sku(),
                  event.quantity());
          String outcome = changed == 1 ? "RESERVED" : "REJECTED_OUT_OF_STOCK";
          jdbc.update(
              "INSERT INTO inventory_reservations"
                  + " (reservation_id, event_id, order_id, sku, quantity, outcome)"
                  + " VALUES (?, ?, ?, ?, ?, ?)",
              UUID.randomUUID(),
              eventId,
              event.orderId(),
              event.sku(),
              event.quantity(),
              outcome);
          return changed == 1 ? Outcome.RESERVED : Outcome.REJECTED_OUT_OF_STOCK;
        });
  }

  private byte[] existingHash(UUID eventId) {
    List<byte[]> hashes =
        jdbc.query(
            "SELECT payload_hash FROM consumer_inbox WHERE consumer_name = ? AND event_id = ?",
            (rs, row) -> rs.getBytes(1),
            CONSUMER_NAME,
            eventId);
    return hashes.isEmpty() ? null : hashes.get(0);
  }

  private Outcome duplicateOrMismatch(
      ConsumerRecord<String, byte[]> record,
      UUID eventId,
      byte[] raw,
      byte[] payloadHash,
      byte[] previousHash) {
    if (previousHash != null && MessageDigest.isEqual(previousHash, payloadHash)) {
      return Outcome.DUPLICATE;
    }
    return deadLetter(record, eventId, "event_id was previously seen with a different payload", raw);
  }

  private Outcome deadLetter(
      ConsumerRecord<String, byte[]> record, UUID eventId, String reason, byte[] raw) {
    jdbc.update(
        "INSERT INTO dead_letters"
            + " (consumer_name, event_id, topic, partition, \"offset\", reason, raw_message,"
            + " record_key, headers) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)",
        CONSUMER_NAME,
        eventId,
        record.topic(),
        record.partition(),
        record.offset(),
        reason.length() > 500 ? reason.substring(0, 500) : reason,
        raw,
        record.key(),
        encodeHeaders(record));
    return Outcome.DEAD_LETTERED;
  }

  private String encodeHeaders(ConsumerRecord<String, byte[]> record) {
    try {
      Map<String, List<String>> headers = new LinkedHashMap<>();
      for (Header header : record.headers()) {
        headers.computeIfAbsent(header.key(), ignored -> new ArrayList<>())
            .add(Base64.getEncoder().encodeToString(header.value()));
      }
      return mapper.writeValueAsString(headers);
    } catch (Exception e) {
      throw new IllegalStateException("could not preserve Kafka headers for dead letter", e);
    }
  }

  private Event parse(String payload, String eventType, int schemaVersion) {
    try {
      if (!"order.created".equals(eventType)) {
        throw new InvalidEventException("unsupported event_type: " + eventType);
      }
      if (schemaVersion != 1) {
        throw new InvalidEventException("unsupported schema_version: " + schemaVersion);
      }
      if (payload == null || payload.isBlank()) {
        throw new InvalidEventException("empty event payload");
      }
      JsonNode json = mapper.readTree(payload);
      if (json == null || !json.isObject()) {
        throw new InvalidEventException("event payload must be a JSON object");
      }
      UUID orderId = UUID.fromString(requiredText(json, "order_id"));
      String sku = requiredText(json, "sku");
      JsonNode quantityNode = json.get("quantity");
      if (sku.length() > 64) {
        throw new InvalidEventException("sku exceeds 64 characters");
      }
      if (quantityNode == null
          || !quantityNode.isIntegralNumber()
          || !quantityNode.canConvertToInt()) {
        throw new InvalidEventException("quantity must be an integer");
      }
      int quantity = quantityNode.intValue();
      if (quantity < 1 || quantity > 1000) {
        throw new InvalidEventException("quantity is outside the supported range");
      }
      return new Event(orderId, sku, quantity);
    } catch (InvalidEventException e) {
      throw e;
    } catch (Exception e) {
      throw new InvalidEventException("invalid order.created payload", e);
    }
  }

  private static String decodeUtf8(byte[] payload) {
    try {
      return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(payload)).toString();
    } catch (CharacterCodingException e) {
      throw new InvalidEventException("event payload is not valid UTF-8", e);
    }
  }

  private static String requiredText(JsonNode json, String field) {
    JsonNode value = json.get(field);
    if (value == null || !value.isTextual() || value.asText().isBlank()) {
      throw new InvalidEventException("missing or invalid " + field);
    }
    return value.asText();
  }

  private static UUID uuidHeader(ConsumerRecord<?, ?> record, String name) {
    String value = stringHeader(record, name);
    try {
      return value == null ? null : UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static int intHeader(ConsumerRecord<?, ?> record, String name, int fallback) {
    String value = stringHeader(record, name);
    try {
      return value == null ? fallback : Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return fallback;
    }
  }

  private static String stringHeader(ConsumerRecord<?, ?> record, String name) {
    Header header = record.headers().lastHeader(name);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  private static byte[] sha256(byte[] raw) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(raw);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  public enum Outcome {
    RESERVED,
    REJECTED_OUT_OF_STOCK,
    DUPLICATE,
    DEAD_LETTERED
  }

  private record Event(UUID orderId, String sku, int quantity) {}

  private static final class InvalidEventException extends RuntimeException {
    private InvalidEventException(String message) {
      super(message);
    }

    private InvalidEventException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
