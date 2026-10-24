package io.latchkey.orders;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.latchkey.fault.FailPoints;
import io.latchkey.support.ApiTestBase;
import java.net.http.HttpResponse;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OrderApiTest extends ApiTestBase {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Autowired FailPoints failPoints;

  @AfterEach
  void clearFailPoints() {
    failPoints.set(FailPoints.BEFORE_COMMIT, null);
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_RESPONSE, null);
  }

  @Test
  void newKeyCreatesOrderIdempotencyRecordAndOutboxRowTogether() throws Exception {
    HttpResponse<String> r = post(KEY_A, newKey(), BODY);

    assertThat(r.statusCode()).isEqualTo(201);
    JsonNode body = JSON.readTree(r.body());
    String operationId = body.get("operation_id").asText();
    assertThat(header(r, "Location")).isEqualTo("/v1/operations/" + operationId);
    assertThat(header(r, "Idempotent-Replayed")).isNull();
    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
    assertThat(count("idempotency_records")).isEqualTo(1);
    assertThat(
            jdbc.queryForObject(
                "SELECT status FROM idempotency_records WHERE operation_id = ?::uuid",
                String.class,
                operationId))
        .isEqualTo("COMPLETED");
    // The outbox row carries a stable event id, the aggregate key and version 1.
    assertThat(
            jdbc.queryForObject(
                "SELECT aggregate_version FROM outbox_events", Integer.class))
        .isEqualTo(1);
  }

  @Test
  void sequentialReplayReturnsIdenticalStatusAndBodyAndCreatesNothingNew() {
    String key = newKey();
    HttpResponse<String> first = post(KEY_A, key, BODY);
    HttpResponse<String> second = post(KEY_A, key, BODY);
    HttpResponse<String> third = post(KEY_A, key, "{ \"quantity\" : 2,\n \"sku\":\"demo-sku\" }");

    assertThat(first.statusCode()).isEqualTo(201);
    for (HttpResponse<String> replay : java.util.List.of(second, third)) {
      assertThat(replay.statusCode()).isEqualTo(201);
      assertThat(replay.body()).isEqualTo(first.body());
      assertThat(header(replay, "Idempotent-Replayed")).isEqualTo("true");
    }
    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
  }

  @Test
  void sameKeyDifferentPayloadReturns422AndLeavesStateUnchanged() {
    String key = newKey();
    HttpResponse<String> first = post(KEY_A, key, BODY);
    HttpResponse<String> changed = post(KEY_A, key, "{\"sku\":\"demo-sku\",\"quantity\":3}");

    assertThat(changed.statusCode()).isEqualTo(422);
    assertThat(changed.body()).contains("IDEMPOTENCY_KEY_REUSED");
    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
    // The original result is still what a correct retry sees.
    assertThat(post(KEY_A, key, BODY).body()).isEqualTo(first.body());
  }

  @Test
  void invalidKeysAreRejectedBeforeAnythingIsStored() {
    assertThat(post(KEY_A, null, BODY).statusCode()).isEqualTo(400);
    assertThat(post(KEY_A, "k".repeat(256), BODY).statusCode()).isEqualTo(400);
    assertThat(post(KEY_A, "has space", BODY).statusCode()).isEqualTo(400);
    assertThat(post(KEY_A, null, BODY).body()).contains("INVALID_IDEMPOTENCY_KEY");
    assertThat(count("idempotency_records")).isZero();
    assertThat(count("orders")).isZero();
  }

  @Test
  void invalidBodiesAreRejectedAndNotStored() {
    String[] bad = {
      "{not json",
      "[]",
      "{}",
      "{\"sku\":\"demo-sku\"}",
      "{\"sku\":\"demo-sku\",\"quantity\":0}",
      "{\"sku\":\"demo-sku\",\"quantity\":-5}",
      "{\"sku\":\"demo-sku\",\"quantity\":1001}",
      "{\"sku\":\"demo-sku\",\"quantity\":1.5}",
      "{\"sku\":\"\",\"quantity\":1}",
      "{\"sku\":\"" + "s".repeat(65) + "\",\"quantity\":1}",
      "{\"sku\":\"demo-sku\",\"quantity\":1,\"extra\":true}",
      ""
    };
    for (String body : bad) {
      HttpResponse<String> r = post(KEY_A, newKey(), body);
      assertThat(r.statusCode()).as("body: %s", body).isEqualTo(400);
      assertThat(r.body()).contains("INVALID_REQUEST");
    }
    assertThat(count("idempotency_records")).isZero();
  }

  @Test
  void oversizedBodyIs413() {
    String huge = "{\"sku\":\"" + "x".repeat(20_000) + "\",\"quantity\":1}";
    HttpResponse<String> r = post(KEY_A, newKey(), huge);
    assertThat(r.statusCode()).isEqualTo(413);
    assertThat(count("idempotency_records")).isZero();
  }

  @Test
  void missingOrWrongCredentialIs401() {
    assertThat(post(null, newKey(), BODY).statusCode()).isEqualTo(401);
    assertThat(post("wrong-key", newKey(), BODY).statusCode()).isEqualTo(401);
    assertThat(get(null, "/v1/operations/" + UUID.randomUUID()).statusCode()).isEqualTo(401);
    assertThat(count("idempotency_records")).isZero();
  }

  @Test
  void unknownSkuIsStoredAsTerminalFailureAndReplayed() {
    String key = newKey();
    String body = "{\"sku\":\"no-such-sku\",\"quantity\":1}";
    HttpResponse<String> first = post(KEY_A, key, body);
    HttpResponse<String> second = post(KEY_A, key, body);

    assertThat(first.statusCode()).isEqualTo(422);
    assertThat(first.body()).contains("UNKNOWN_SKU");
    assertThat(second.statusCode()).isEqualTo(422);
    assertThat(second.body()).isEqualTo(first.body());
    assertThat(header(second, "Idempotent-Replayed")).isEqualTo("true");
    assertThat(count("orders")).isZero();
    assertThat(count("outbox_events")).isZero();
    assertThat(jdbc.queryForObject("SELECT status FROM idempotency_records", String.class))
        .isEqualTo("FAILED_TERMINAL");
  }

  @Test
  void tenantsAreIsolated() throws Exception {
    String key = newKey();
    HttpResponse<String> a = post(KEY_A, key, BODY);
    HttpResponse<String> b = post(KEY_B, key, BODY); // same key string, other tenant: no collision

    assertThat(a.statusCode()).isEqualTo(201);
    assertThat(b.statusCode()).isEqualTo(201);
    assertThat(header(b, "Idempotent-Replayed")).isNull();
    assertThat(count("orders")).isEqualTo(2);

    String aOperation = JSON.readTree(a.body()).get("operation_id").asText();
    assertThat(get(KEY_A, "/v1/operations/" + aOperation).statusCode()).isEqualTo(200);
    HttpResponse<String> crossRead = get(KEY_B, "/v1/operations/" + aOperation);
    assertThat(crossRead.statusCode()).isEqualTo(404);
    assertThat(crossRead.body()).doesNotContain(aOperation);
  }

  @Test
  void operationEndpointReturnsStoredResult() throws Exception {
    HttpResponse<String> created = post(KEY_A, newKey(), BODY);
    String operationId = JSON.readTree(created.body()).get("operation_id").asText();

    HttpResponse<String> r = get(KEY_A, "/v1/operations/" + operationId);
    assertThat(r.statusCode()).isEqualTo(200);
    JsonNode op = JSON.readTree(r.body());
    assertThat(op.get("status").asText()).isEqualTo("COMPLETED");
    assertThat(op.get("response_code").asInt()).isEqualTo(201);
    assertThat(op.get("response_body")).isEqualTo(JSON.readTree(created.body()));

    assertThat(get(KEY_A, "/v1/operations/not-a-uuid").statusCode()).isEqualTo(404);
    assertThat(get(KEY_A, "/v1/operations/" + UUID.randomUUID()).statusCode()).isEqualTo(404);
  }

  @Test
  void failureBeforeCommitLeavesNothingBehindAndRetrySucceeds() {
    String key = newKey();
    failPoints.set(FailPoints.BEFORE_COMMIT, "error");
    HttpResponse<String> crashed = post(KEY_A, key, BODY);

    assertThat(crashed.statusCode()).isEqualTo(500);
    assertThat(count("idempotency_records")).isZero();
    assertThat(count("orders")).isZero();
    assertThat(count("outbox_events")).isZero();

    failPoints.set(FailPoints.BEFORE_COMMIT, null);
    HttpResponse<String> retry = post(KEY_A, key, BODY);
    assertThat(retry.statusCode()).isEqualTo(201);
    assertThat(header(retry, "Idempotent-Replayed")).isNull();
    assertThat(count("orders")).isEqualTo(1);
  }

  @Test
  void failureAfterCommitBeforeResponseRetryReturnsStoredResultWithOneOrder() {
    String key = newKey();
    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_RESPONSE, "error");
    HttpResponse<String> lostResponse = post(KEY_A, key, BODY);
    assertThat(lostResponse.statusCode()).isEqualTo(500); // client never saw the 201

    assertThat(count("orders")).isEqualTo(1); // but the work committed

    failPoints.set(FailPoints.AFTER_COMMIT_BEFORE_RESPONSE, null);
    HttpResponse<String> retry = post(KEY_A, key, BODY);
    assertThat(retry.statusCode()).isEqualTo(201);
    assertThat(header(retry, "Idempotent-Replayed")).isEqualTo("true");
    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
  }

  @Test
  void expiredKeyIsTreatedAsNewRequest() throws Exception {
    String key = newKey();
    HttpResponse<String> first = post(KEY_A, key, BODY);
    jdbc.update("UPDATE idempotency_records SET expires_at = now() - interval '1 second'");

    HttpResponse<String> second = post(KEY_A, key, BODY);

    assertThat(second.statusCode()).isEqualTo(201);
    assertThat(header(second, "Idempotent-Replayed")).isNull();
    assertThat(JSON.readTree(second.body()).get("operation_id"))
        .isNotEqualTo(JSON.readTree(first.body()).get("operation_id"));
    assertThat(count("orders")).isEqualTo(2);
  }

  @Test
  void unsupportedContentTypeIs415() throws Exception {
    java.net.http.HttpRequest request =
        java.net.http.HttpRequest.newBuilder(
                java.net.URI.create("http://localhost:" + port + "/v1/orders"))
            .header("Authorization", "Bearer " + KEY_A)
            .header("Idempotency-Key", newKey())
            .header("Content-Type", "text/plain")
            .POST(java.net.http.HttpRequest.BodyPublishers.ofString(BODY))
            .build();
    assertThat(send(request).statusCode()).isEqualTo(415);
  }
}
