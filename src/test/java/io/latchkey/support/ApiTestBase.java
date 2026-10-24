package io.latchkey.support;

import io.latchkey.dev.DevSeeder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Boots the real order-api against a real PostgreSQL. The connection pool is deliberately small so
 * concurrency tests also prove nothing deadlocks when threads outnumber connections.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("api")
public abstract class ApiTestBase {

  public static final String KEY_A = "test-key-tenant-a";
  public static final String KEY_B = "test-key-tenant-b";
  public static final String BODY = "{\"sku\":\"demo-sku\",\"quantity\":2}";

  // One container for the whole test run (singleton pattern); Ryuk removes it afterwards.
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  static {
    POSTGRES.start();
  }

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", POSTGRES::getUsername);
    registry.add("spring.datasource.password", POSTGRES::getPassword);
    registry.add("spring.datasource.hikari.maximum-pool-size", () -> "5");
    registry.add("management.server.port", () -> "0");
    registry.add("latchkey.seed.enabled", () -> "true");
    registry.add("latchkey.seed.key-a", () -> KEY_A);
    registry.add("latchkey.seed.key-b", () -> KEY_B);
    registry.add("latchkey.seed.skus", () -> "demo-sku,other-sku");
    registry.add("latchkey.failpoints-enabled", () -> "true");
    registry.add("latchkey.lock-timeout-ms", () -> "5000");
  }

  @LocalServerPort protected int port;
  @Autowired protected JdbcTemplate jdbc;

  protected final HttpClient http =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @BeforeEach
  void cleanData() {
    jdbc.execute("TRUNCATE orders, outbox_events, idempotency_records");
  }

  protected static String newKey() {
    return UUID.randomUUID().toString();
  }

  protected HttpResponse<String> post(String apiKey, String idempotencyKey, String body) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/orders"))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body));
    if (apiKey != null) {
      request.header("Authorization", "Bearer " + apiKey);
    }
    if (idempotencyKey != null) {
      request.header("Idempotency-Key", idempotencyKey);
    }
    return send(request.build());
  }

  protected HttpResponse<String> get(String apiKey, String path) {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .timeout(Duration.ofSeconds(30))
            .GET();
    if (apiKey != null) {
      request.header("Authorization", "Bearer " + apiKey);
    }
    return send(request.build());
  }

  protected HttpResponse<String> send(HttpRequest request) {
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  protected long count(String table) {
    Long n = jdbc.queryForObject("SELECT count(*) FROM " + table, Long.class);
    return n == null ? 0 : n;
  }

  protected static String header(HttpResponse<?> response, String name) {
    return response.headers().firstValue(name).orElse(null);
  }

  protected static final UUID TENANT_A = DevSeeder.TENANT_A;
  protected static final UUID TENANT_B = DevSeeder.TENANT_B;
}
