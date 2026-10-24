package io.latchkey.orders;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.UUID;

/** What the controller should answer. {@code replayed} adds Idempotent-Replayed: true. */
public record OrderResult(int statusCode, JsonNode body, boolean replayed, UUID operationId) {}
