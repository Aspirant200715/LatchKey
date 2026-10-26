package io.latchkey.orders;

import org.springframework.context.annotation.Profile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.latchkey.auth.Tenant;
import io.latchkey.fault.FailPoints;
import io.latchkey.idempotency.IdempotencyKeyValidator;
import io.latchkey.idempotency.InvalidPayloadException;
import io.latchkey.idempotency.PayloadHasher;
import io.latchkey.web.ApiException;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Mode A (local-only) idempotent order creation. The idempotency record, the order and the outbox
 * row commit in ONE transaction, so an IN_PROGRESS row is never visible. A concurrent duplicate
 * blocks on the unique index until the first transaction ends, bounded by lock_timeout.
 */
@Profile("api")
@Service
public class OrderService {

  public static final String OPERATION = "create_order";
  public static final int MAX_BODY_BYTES = 16 * 1024;

  private static final Logger log = LoggerFactory.getLogger(OrderService.class);

  private final JdbcTemplate jdbc;
  private final TransactionTemplate tx;
  private final ObjectMapper mapper;
  private final IdempotencyKeyValidator keyValidator;
  private final PayloadHasher hasher;
  private final FailPoints failPoints;
  private final long lockTimeoutMs;
  private final double ttlSeconds;

  public OrderService(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      ObjectMapper mapper,
      IdempotencyKeyValidator keyValidator,
      PayloadHasher hasher,
      FailPoints failPoints,
      @Value("${latchkey.lock-timeout-ms:2000}") long lockTimeoutMs,
      @Value("${latchkey.idempotency-ttl-hours:24}") long ttlHours) {
    this.jdbc = jdbc;
    this.tx = tx;
    this.mapper =
        mapper
            .copy()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);
    this.keyValidator = keyValidator;
    this.hasher = hasher;
    this.failPoints = failPoints;
    this.lockTimeoutMs = lockTimeoutMs;
    this.ttlSeconds = ttlHours * 3600d;
  }

  public OrderResult createOrder(Tenant tenant, String rawKey, byte[] rawBody, String traceparent) {
    // Everything below up to the transaction is pure validation: nothing is stored on failure.
    String key = keyValidator.validate(rawKey);
    CreateOrderRequest request = parseAndValidate(rawBody);
    byte[] hash;
    try {
      hash = hasher.hash(OPERATION, rawBody);
    } catch (InvalidPayloadException e) {
      throw invalidRequest(e.getMessage());
    }

    OrderResult result;
    try {
      result = tx.execute(status -> claim(tenant, key, request, hash, traceparent));
    } catch (DataAccessException | TransactionException e) {
      throw translate(e);
    }
    failPoints.hit(FailPoints.AFTER_COMMIT_BEFORE_RESPONSE);
    return result;
  }

  private OrderResult claim(
      Tenant tenant, String key, CreateOrderRequest request, byte[] hash, String traceparent) {
    jdbc.queryForObject(
        "SELECT set_config('lock_timeout', ?, true)", String.class, lockTimeoutMs + "ms");
    boolean skuKnown =
        Boolean.TRUE.equals(
            jdbc.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM skus WHERE sku = ?)", Boolean.class, request.sku()));

    // Two attempts: the second covers "expired record removed, claim again".
    for (int attempt = 0; attempt < 2; attempt++) {
      UUID operationId = UUID.randomUUID();
      UUID orderId = UUID.randomUUID();
      String status = skuKnown ? "COMPLETED" : "FAILED_TERMINAL";
      int code = skuKnown ? 201 : 422;
      ObjectNode body = skuKnown ? createdBody(operationId, orderId) : unknownSkuBody();

      // RETURNING the stored text matters: jsonb may reorder keys, and the first answer must be
      // byte-identical to every later replay.
      List<String> inserted =
          jdbc.query(
              "INSERT INTO idempotency_records (tenant_id, operation_name, idempotency_key,"
                  + " operation_id, payload_hash, status, response_code, response_body, expires_at)"
                  + " VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, now() + make_interval(secs => ?))"
                  + " ON CONFLICT (tenant_id, operation_name, idempotency_key) DO NOTHING"
                  + " RETURNING response_body::text",
              (rs, i) -> rs.getString(1),
              tenant.tenantId(),
              OPERATION,
              key,
              operationId,
              hash,
              status,
              code,
              body.toString(),
              ttlSeconds);

      if (!inserted.isEmpty()) {
        if (skuKnown) {
          insertOrderAndOutbox(tenant, operationId, orderId, request, traceparent);
        }
        failPoints.hit(FailPoints.BEFORE_COMMIT);
        return new OrderResult(code, readJson(inserted.get(0)), false, operationId);
      }

      // Conflict: the winning row is committed (our insert waited for it if it was still open).
      List<Existing> rows =
          jdbc.query(
              "SELECT operation_id, payload_hash, status, response_code, response_body::text,"
                  + " expires_at < now() AS expired FROM idempotency_records"
                  + " WHERE tenant_id = ? AND operation_name = ? AND idempotency_key = ?",
              (rs, i) ->
                  new Existing(
                      rs.getObject("operation_id", UUID.class),
                      rs.getBytes("payload_hash"),
                      rs.getString("status"),
                      rs.getInt("response_code"),
                      rs.getString("response_body"),
                      rs.getBoolean("expired")),
              tenant.tenantId(),
              OPERATION,
              key);
      if (rows.isEmpty()) {
        continue; // removed concurrently by cleanup; claim again
      }
      Existing existing = rows.get(0);
      if (existing.expired()) {
        jdbc.update(
            "DELETE FROM idempotency_records WHERE tenant_id = ? AND operation_name = ?"
                + " AND idempotency_key = ? AND expires_at < now()",
            tenant.tenantId(),
            OPERATION,
            key);
        continue;
      }
      if (!PayloadHasher.matches(existing.payloadHash(), hash)) {
        throw new ApiException(
            HttpStatus.UNPROCESSABLE_ENTITY,
            "IDEMPOTENCY_KEY_REUSED",
            "Idempotency-Key was already used with a different request body");
      }
      if ("IN_PROGRESS".equals(existing.status())) {
        throw inFlight();
      }
      return new OrderResult(
          existing.responseCode(), readJson(existing.responseBody()), true, existing.operationId());
    }
    throw inFlight();
  }

  private void insertOrderAndOutbox(
      Tenant tenant, UUID operationId, UUID orderId, CreateOrderRequest request, String traceparent) {
    jdbc.update(
        "INSERT INTO orders (order_id, tenant_id, operation_id, sku, quantity, status)"
            + " VALUES (?, ?, ?, ?, ?, 'CREATED')",
        orderId,
        tenant.tenantId(),
        operationId,
        request.sku(),
        request.quantity());

    ObjectNode payload = mapper.createObjectNode();
    payload.put("order_id", orderId.toString());
    payload.put("tenant_id", tenant.tenantId().toString());
    payload.put("operation_id", operationId.toString());
    payload.put("sku", request.sku());
    payload.put("quantity", request.quantity());
    ObjectNode headers = mapper.createObjectNode();
    if (traceparent != null && !traceparent.isBlank()) {
      headers.put("traceparent", traceparent);
    }

    // event_id is generated HERE, inside the producing transaction, and never regenerated.
    jdbc.update(
        "INSERT INTO outbox_events (event_id, tenant_id, aggregate_type, aggregate_id,"
            + " aggregate_version, partition_key, event_type, payload, headers)"
            + " VALUES (?, ?, 'order', ?, 1, ?, 'order.created', ?::jsonb, ?::jsonb)",
        UUID.randomUUID(),
        tenant.tenantId(),
        orderId,
        orderId.toString(),
        payload.toString(),
        headers.toString());
  }

  private CreateOrderRequest parseAndValidate(byte[] rawBody) {
    if (rawBody == null || rawBody.length == 0) {
      throw invalidRequest("request body is required");
    }
    if (rawBody.length > MAX_BODY_BYTES) {
      throw new ApiException(
          HttpStatus.PAYLOAD_TOO_LARGE,
          "PAYLOAD_TOO_LARGE",
          "body must be at most " + MAX_BODY_BYTES + " bytes");
    }
    CreateOrderRequest request;
    try {
      request = mapper.readValue(rawBody, CreateOrderRequest.class);
    } catch (java.io.IOException e) {
      throw invalidRequest("body must be JSON of the form {\"sku\": string, \"quantity\": integer}");
    }
    if (request == null || request.sku() == null || request.sku().isBlank()
        || request.sku().length() > CreateOrderRequest.MAX_SKU_LENGTH) {
      throw invalidRequest("sku must be 1-" + CreateOrderRequest.MAX_SKU_LENGTH + " characters");
    }
    if (request.quantity() < 1 || request.quantity() > CreateOrderRequest.MAX_QUANTITY) {
      throw invalidRequest("quantity must be between 1 and " + CreateOrderRequest.MAX_QUANTITY);
    }
    return request;
  }

  private ObjectNode createdBody(UUID operationId, UUID orderId) {
    ObjectNode body = mapper.createObjectNode();
    body.put("operation_id", operationId.toString());
    body.put("order_id", orderId.toString());
    body.put("status", "CREATED");
    return body;
  }

  private ObjectNode unknownSkuBody() {
    ObjectNode error = mapper.createObjectNode();
    error.put("code", "UNKNOWN_SKU");
    error.put("message", "sku is not in the catalog");
    ObjectNode body = mapper.createObjectNode();
    body.set("error", error);
    return body;
  }

  private JsonNode readJson(String json) {
    try {
      return mapper.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("stored response is not valid JSON", e);
    }
  }

  private static ApiException invalidRequest(String message) {
    return new ApiException(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", message);
  }

  private static ApiException inFlight() {
    return new ApiException(
        HttpStatus.CONFLICT,
        "REQUEST_IN_FLIGHT",
        "a request with this Idempotency-Key is still being processed",
        1);
  }

  /** 55P03 (lock timeout) => 409 retryable; any other database trouble => 503, never cached. */
  private ApiException translate(RuntimeException e) {
    String sqlState = null;
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof SQLException sql) {
        sqlState = sql.getSQLState();
        break;
      }
    }
    if ("55P03".equals(sqlState)) {
      return inFlight();
    }
    log.warn("database error handling create_order: {} sqlstate={}", e.getClass().getSimpleName(), sqlState);
    return new ApiException(
        HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", "service temporarily unavailable", 1);
  }

  private record Existing(
      UUID operationId,
      byte[] payloadHash,
      String status,
      int responseCode,
      String responseBody,
      boolean expired) {}
}
