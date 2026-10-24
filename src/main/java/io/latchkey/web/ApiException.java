package io.latchkey.web;

import org.springframework.http.HttpStatus;

/** An error with a stable machine-readable code that maps to one HTTP response. */
public class ApiException extends RuntimeException {

  private final HttpStatus status;
  private final String code;
  private final Integer retryAfterSeconds;

  public ApiException(HttpStatus status, String code, String message) {
    this(status, code, message, null);
  }

  public ApiException(HttpStatus status, String code, String message, Integer retryAfterSeconds) {
    super(message);
    this.status = status;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }

  public HttpStatus status() {
    return status;
  }

  public String code() {
    return code;
  }

  public Integer retryAfterSeconds() {
    return retryAfterSeconds;
  }
}
