package io.latchkey.relay;

import java.time.Instant;
import java.util.UUID;

public record OutboxRow(
    UUID eventId,
    UUID tenantId,
    String aggregateType,
    UUID aggregateId,
    int aggregateVersion,
    String partitionKey,
    String eventType,
    int schemaVersion,
    String payload,
    String headers,
    Instant createdAt,
    int attempts) {}
