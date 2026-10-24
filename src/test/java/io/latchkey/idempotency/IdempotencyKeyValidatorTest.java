package io.latchkey.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.latchkey.web.ApiException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class IdempotencyKeyValidatorTest {

  private final IdempotencyKeyValidator validator = new IdempotencyKeyValidator();

  @Test
  void acceptsUuidAndBoundaryLengths() {
    assertThat(validator.validate("3f1c2a9e-8f7b-4d1a-9c55-0a1b2c3d4e5f")).isNotBlank();
    assertThat(validator.validate("a")).isEqualTo("a");
    assertThat(validator.validate("k".repeat(255))).hasSize(255);
  }

  @Test
  void rejectsMissingEmptyAndTooLong() {
    assertRejected(null);
    assertRejected("");
    assertRejected("k".repeat(256));
  }

  @Test
  void rejectsNonVisibleAsciiAndSpaces() {
    assertRejected("has space");
    assertRejected("tab\tkey");
    assertRejected("new\nline");
    assertRejected("café");
    assertRejected("emoji😀");
    assertRejected("del\u007f");
  }

  private void assertRejected(String key) {
    assertThatThrownBy(() -> validator.validate(key))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.status()).isEqualTo(HttpStatus.BAD_REQUEST);
              assertThat(e.code()).isEqualTo("INVALID_IDEMPOTENCY_KEY");
            });
  }
}
