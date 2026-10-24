package io.latchkey.idempotency;

import io.latchkey.web.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

/** Validates the Idempotency-Key header before any database work. */
@Component
public class IdempotencyKeyValidator {

  public static final int MAX_LENGTH = 255;

  /** Returns the key if valid; throws 400 INVALID_IDEMPOTENCY_KEY otherwise. */
  public String validate(String key) {
    if (key == null || key.isEmpty()) {
      throw invalid("Idempotency-Key header is required");
    }
    if (key.length() > MAX_LENGTH) {
      throw invalid("Idempotency-Key must be at most " + MAX_LENGTH + " characters");
    }
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c < 0x21 || c > 0x7E) {
        throw invalid("Idempotency-Key must contain only visible ASCII characters");
      }
    }
    return key;
  }

  private static ApiException invalid(String message) {
    return new ApiException(HttpStatus.BAD_REQUEST, "INVALID_IDEMPOTENCY_KEY", message);
  }
}
