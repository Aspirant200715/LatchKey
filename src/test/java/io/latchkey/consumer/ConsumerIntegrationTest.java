package io.latchkey.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.latchkey.fault.FailPoints;
import io.latchkey.support.ConsumerTestBase;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class ConsumerIntegrationTest extends ConsumerTestBase {

  private String sku;

  @Autowired private FailPoints failPoints;

  @BeforeEach
  void prepareInventory() {
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_ACK, null);
    sku = "consumer-test-" + UUID.randomUUID();
    seedInventory(sku, 10);
  }

  @AfterEach
  void clearFailPoint() {
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_ACK, null);
  }

  @Test
  void reservesStockAndDuplicateDeliveryCreatesOnlyOneEffect() {
    UUID eventId = UUID.randomUUID();
    publish(eventId, sku, 2, 1);
    publish(eventId, sku, 2, 1);

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(reservationCount(eventId)).isEqualTo(1);
              assertThat(stock()).isEqualTo(8);
            });
    assertThat(inboxCount(eventId)).isEqualTo(1);
  }

  @Test
  void outOfStockIsRecordedWithoutMakingStockNegative() {
    jdbc.update("UPDATE inventory SET stock = 1 WHERE sku = ?", sku);
    UUID eventId = UUID.randomUUID();
    publish(eventId, sku, 2, 1);

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () ->
                assertThat(
                        jdbc.queryForList(
                            "SELECT outcome FROM inventory_reservations WHERE event_id = ?",
                            String.class,
                            eventId))
                    .containsExactly("REJECTED_OUT_OF_STOCK"));
    assertThat(stock()).isEqualTo(1);
  }

  @Test
  void poisonEventIsDeadLetteredAndCanBeReplayedWithTheSameEventId() {
    UUID eventId = UUID.randomUUID();
    publish(eventId, sku, 2, 99); // unsupported schema version

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(deadLetterCount(eventId)).isEqualTo(1));
    assertThat(inboxCount(eventId)).isZero();
    assertThat(reservationCount(eventId)).isZero();

    publish(eventId, sku, 2, 1); // replay after correction; keep the stable event id
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(reservationCount(eventId)).isEqualTo(1);
              assertThat(stock()).isEqualTo(8);
            });
  }

  @Test
  void payloadMismatchForKnownEventIdIsDeadLetteredWithoutRepeatingEffect() {
    UUID eventId = UUID.randomUUID();
    publish(eventId, sku, 1, 1);
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(reservationCount(eventId)).isEqualTo(1));

    publish(eventId, sku, 3, 1);
    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(() -> assertThat(deadLetterCount(eventId)).isEqualTo(1));
    assertThat(reservationCount(eventId)).isEqualTo(1);
    assertThat(stock()).isEqualTo(9);
  }

  @Test
  void crashAfterDatabaseCommitBeforeAckRedeliversAsNoOp() {
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_ACK, "error");
    UUID eventId = UUID.randomUUID();
    publish(eventId, sku, 2, 1);

    await().atMost(Duration.ofSeconds(30))
        .untilAsserted(
            () -> {
              assertThat(reservationCount(eventId)).isEqualTo(1);
              assertThat(stock()).isEqualTo(8);
            });
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_ACK, null);
    assertThat(inboxCount(eventId)).isEqualTo(1);
  }

  private int stock() {
    return jdbc.queryForObject("SELECT stock FROM inventory WHERE sku = ?", Integer.class, sku);
  }

  private long reservationCount(UUID eventId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM inventory_reservations WHERE event_id = ?", Long.class, eventId);
  }

  private long inboxCount(UUID eventId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM consumer_inbox WHERE consumer_name = ? AND event_id = ?",
        Long.class,
        "inventory-reservations-v1",
        eventId);
  }

  private long deadLetterCount(UUID eventId) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM dead_letters WHERE event_id = ?", Long.class, eventId);
  }
}
