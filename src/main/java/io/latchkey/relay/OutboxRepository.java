package io.latchkey.relay;

import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Outbox access for the relay. Every method is its own short auto-commit statement on purpose: the
 * claim commits BEFORE publishing, so the lease (locked_until) is what protects against a dead
 * worker. Publishing may therefore repeat; consumers deduplicate by event_id.
 */
@Repository
@Profile("relay")
public class OutboxRepository {

  private final JdbcTemplate jdbc;

  public OutboxRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /** Claims up to {@code limit} due rows. Rows with an expired lease become claimable again. */
  public List<OutboxRow> claimBatch(int limit, String workerId, double leaseSeconds) {
    List<OutboxRow> rows =
        jdbc.query(
            "WITH batch AS ("
                + " SELECT event_id FROM outbox_events"
                + " WHERE published_at IS NULL AND next_attempt_at <= now()"
                + " AND (locked_until IS NULL OR locked_until < now())"
                + " ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED)"
                + " UPDATE outbox_events o SET locked_by = ?,"
                + " locked_until = now() + make_interval(secs => ?), attempts = o.attempts + 1"
                + " FROM batch WHERE o.event_id = batch.event_id"
                + " RETURNING o.event_id, o.tenant_id, o.aggregate_type, o.aggregate_id,"
                + " o.aggregate_version, o.partition_key, o.event_type, o.schema_version,"
                + " o.payload::text AS payload, o.headers::text AS headers, o.created_at,"
                + " o.attempts",
            (rs, i) ->
                new OutboxRow(
                    rs.getObject("event_id", UUID.class),
                    rs.getObject("tenant_id", UUID.class),
                    rs.getString("aggregate_type"),
                    rs.getObject("aggregate_id", UUID.class),
                    rs.getInt("aggregate_version"),
                    rs.getString("partition_key"),
                    rs.getString("event_type"),
                    rs.getInt("schema_version"),
                    rs.getString("payload"),
                    rs.getString("headers"),
                    rs.getTimestamp("created_at").toInstant(),
                    rs.getInt("attempts")),
            limit,
            workerId,
            leaseSeconds);
    // UPDATE ... RETURNING has no defined order; publish oldest first.
    return rows.stream()
        .sorted(Comparator.comparing(OutboxRow::createdAt).thenComparing(OutboxRow::aggregateVersion))
        .toList();
  }

  /** Marks a row published. Safe to repeat: a second call changes nothing. */
  public void markPublished(UUID eventId) {
    jdbc.update(
        "UPDATE outbox_events SET published_at = now(), locked_by = NULL, locked_until = NULL,"
            + " last_error = NULL WHERE event_id = ? AND published_at IS NULL",
        eventId);
  }

  /** Releases the lease and schedules the next try. attempts was already counted at claim time. */
  public void markFailed(UUID eventId, String error, double backoffSeconds) {
    jdbc.update(
        "UPDATE outbox_events SET last_error = ?, locked_by = NULL, locked_until = NULL,"
            + " next_attempt_at = now() + make_interval(secs => ?)"
            + " WHERE event_id = ? AND published_at IS NULL",
        truncate(error),
        backoffSeconds,
        eventId);
  }

  /** On graceful shutdown: give back rows this worker claimed but did not finish. */
  public int releaseLeases(String workerId) {
    return jdbc.update(
        "UPDATE outbox_events SET locked_by = NULL, locked_until = NULL"
            + " WHERE locked_by = ? AND published_at IS NULL",
        workerId);
  }

  private static String truncate(String s) {
    if (s == null) {
      return null;
    }
    return s.length() <= 500 ? s : s.substring(0, 500);
  }
}
