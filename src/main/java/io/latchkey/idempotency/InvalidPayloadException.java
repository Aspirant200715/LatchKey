package io.latchkey.idempotency;

public class InvalidPayloadException extends RuntimeException {
  public InvalidPayloadException(String message) {
    super(message);
  }
}
