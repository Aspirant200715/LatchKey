package io.latchkey.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.latchkey.support.Containers;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** The claim query on its own (worker disabled): SKIP LOCKED, leases and backoff. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("relay")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxRepositoryTest {

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", Containers.POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", Containers.POSTGRES::getUsername);
    registry.add("spring.datasource.password", Containers.POSTGRES::getPassword);
    registry.add("server.port", () -> "0");
    registry.add("management.server.port", () -> "0");
    registry.add("latchkey.relay.enabled", () -> "false"); // no worker: this test drives the claims
  }

  @Autowired OutboxRepository outbox;
  @Autowired JdbcTemplate jdbc;

  @BeforeEach
  void clean() {
    jdbc.execute("TRUNCATE outbox_events");
  }

  private UUID insert() {
    UUID id = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO outbox_events (event_id, tenant_id, aggregate_type, aggregate_id,"
            + " aggregate_version, partition_key, event_type, payload)"
            + " VALUES (?, ?, 'order', ?, 1, ?, 'order.created', '{}'::jsonb)",
        id, UUID.randomUUID(), id, id.toString());
    return id;
  }

  @Test
  void concurrentWorkersNeverClaimTheSameRow() throws Exception {
    for (int i = 0; i < 300; i++) {
      insert();
    }
    ExecutorService pool = Executors.newFixedThreadPool(4);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<List<OutboxRow>>> futures = new java.util.ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final String worker = "w" + w;
      futures.add(pool.submit(() -> {
        go.await();
        return outbox.claimBatch(300, worker, 30);
      }));
    }
    go.countDown();
    Set<UUID> seen = new HashSet<>();
    int total = 0;
    for (Future<List<OutboxRow>> f : futures) {
      for (OutboxRow row : f.get()) {
        total++;
        seen.add(row.eventId());
      }
    }
    pool.shutdown();

    assertThat(total).isEqualTo(300);
    assertThat(seen).hasSize(300); // no row handed to two workers
  }

  @Test
  void liveLeaseBlocksReclaimAndExpiredLeaseAllowsIt() {
    UUID id = insert();

    List<OutboxRow> first = outbox.claimBatch(10, "a", 1);
    assertThat(first).extracting(OutboxRow::eventId).containsExactly(id);
    assertThat(first.get(0).attempts()).isEqualTo(1);
    assertThat(outbox.claimBatch(10, "b", 1)).isEmpty(); // lease still live

    await().atMost(Duration.ofSeconds(10)).until(() -> !outbox.claimBatch(10, "b", 30).isEmpty());
    assertThat(jdbc.queryForObject(
            "SELECT attempts FROM outbox_events WHERE event_id = ?", Integer.class, id))
        .isEqualTo(2);
    assertThat(jdbc.queryForObject(
            "SELECT locked_by FROM outbox_events WHERE event_id = ?", String.class, id))
        .isEqualTo("b");
  }

  @Test
  void failedRowWaitsForItsBackoffBeforeItIsClaimableAgain() {
    UUID id = insert();
    outbox.claimBatch(10, "a", 30);

    outbox.markFailed(id, "broker down", 2.0);

    assertThat(outbox.claimBatch(10, "a", 30)).isEmpty();
    assertThat(jdbc.queryForObject(
            "SELECT last_error FROM outbox_events WHERE event_id = ?", String.class, id))
        .isEqualTo("broker down");
    await().atMost(Duration.ofSeconds(10)).until(() -> !outbox.claimBatch(10, "a", 30).isEmpty());
  }

  @Test
  void publishedRowsAreNeverClaimedAndMarkingTwiceIsHarmless() {
    UUID id = insert();
    outbox.claimBatch(10, "a", 30);

    outbox.markPublished(id);
    outbox.markPublished(id);

    assertThat(unpublished()).isZero();
    assertThat(outbox.claimBatch(10, "a", 30)).isEmpty();
    // A late failure report must not un-publish a row.
    outbox.markFailed(id, "late error", 5);
    assertThat(unpublished()).isZero();
  }

  @Test
  void releaseLeasesGivesBackOnlyUnfinishedRowsOfThatWorker() {
    UUID done = insert();
    UUID open = insert();
    insert(); // claimed by another worker
    outbox.claimBatch(2, "a", 60);
    outbox.claimBatch(10, "other", 60);
    outbox.markPublished(done);

    int released = outbox.releaseLeases("a");

    assertThat(released).isLessThanOrEqualTo(2);
    assertThat(jdbc.queryForObject(
            "SELECT locked_by FROM outbox_events WHERE event_id = ?", String.class, open))
        .isNotEqualTo("a");
    assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM outbox_events WHERE locked_by = 'a'", Long.class))
        .isZero();
  }

  private long unpublished() {
    Long n = jdbc.queryForObject(
        "SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class);
    return n == null ? 0 : n;
  }
}
