package io.latchkey.orders;

import static org.assertj.core.api.Assertions.assertThat;

import io.latchkey.support.ApiTestBase;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** 200 simultaneous requests, one key, a 5-connection pool: exactly one order may exist. */
class HotKeyConcurrencyTest extends ApiTestBase {

  private static final int THREADS = 200;

  @Test
  void samePayloadExactlyOneOrderAndNoServerErrors() throws Exception {
    String key = newKey();
    List<HttpResponse<String>> responses = fireAll(i -> post(KEY_A, key, BODY));

    Map<Integer, Long> byStatus =
        responses.stream().collect(Collectors.groupingBy(HttpResponse::statusCode, Collectors.counting()));
    // 201 (winner or replay) and 409 (waited past lock_timeout) are the only allowed answers.
    assertThat(byStatus.keySet()).isSubsetOf(201, 409);

    long winners = responses.stream()
        .filter(r -> r.statusCode() == 201 && header(r, "Idempotent-Replayed") == null)
        .count();
    assertThat(winners).isEqualTo(1);

    // Every replay carries the winner's exact body.
    String winnerBody = responses.stream()
        .filter(r -> r.statusCode() == 201 && header(r, "Idempotent-Replayed") == null)
        .findFirst().orElseThrow().body();
    assertThat(responses.stream()
            .filter(r -> r.statusCode() == 201)
            .map(HttpResponse::body)
            .distinct())
        .containsExactly(winnerBody);

    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
    assertThat(count("idempotency_records")).isEqualTo(1);
  }

  @Test
  void differentPayloadsExactlyOneWinnerEveryoneElseGets422OrReplay() throws Exception {
    String key = newKey();
    // Quantity varies per thread: five distinct payloads fighting over one key.
    List<HttpResponse<String>> responses =
        fireAll(i -> post(KEY_A, key, "{\"sku\":\"demo-sku\",\"quantity\":" + (1 + i % 5) + "}"));

    Map<Integer, Long> byStatus =
        responses.stream().collect(Collectors.groupingBy(HttpResponse::statusCode, Collectors.counting()));
    assertThat(byStatus.keySet()).isSubsetOf(201, 409, 422);

    long winners = responses.stream()
        .filter(r -> r.statusCode() == 201 && header(r, "Idempotent-Replayed") == null)
        .count();
    assertThat(winners).isEqualTo(1);
    assertThat(byStatus.getOrDefault(422, 0L)).isGreaterThan(0);

    assertThat(count("orders")).isEqualTo(1);
    assertThat(count("outbox_events")).isEqualTo(1);
  }

  @Test
  void manyDistinctKeysAllSucceedWithOneOrderEach() throws Exception {
    List<HttpResponse<String>> responses = fireAll(i -> post(KEY_A, newKey(), BODY));

    assertThat(responses).allSatisfy(r -> assertThat(r.statusCode()).isEqualTo(201));
    assertThat(count("orders")).isEqualTo(THREADS);
    assertThat(count("outbox_events")).isEqualTo(THREADS);
    assertThat(jdbc.queryForObject("SELECT count(DISTINCT event_id) FROM outbox_events", Long.class))
        .isEqualTo(THREADS);
  }

  private List<HttpResponse<String>> fireAll(IntFunction<HttpResponse<String>> request)
      throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    CountDownLatch ready = new CountDownLatch(THREADS);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<HttpResponse<String>>> futures = new ArrayList<>();
    for (int i = 0; i < THREADS; i++) {
      final int n = i;
      Callable<HttpResponse<String>> task =
          () -> {
            ready.countDown();
            go.await();
            return request.apply(n);
          };
      futures.add(pool.submit(task));
    }
    ready.await();
    go.countDown();
    List<HttpResponse<String>> responses = new ArrayList<>();
    for (Future<HttpResponse<String>> f : futures) {
      responses.add(f.get());
    }
    pool.shutdown();
    return responses;
  }
}
