package io.latchkey.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.latchkey.fault.FailPointException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/** Every error leaves as {"error":{"code","message"}} with a stable code. Never echoes input. */
@RestControllerAdvice
public class ApiExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

  private final ObjectMapper mapper;

  public ApiExceptionHandler(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @ExceptionHandler(ApiException.class)
  public ResponseEntity<ObjectNode> api(ApiException e) {
    return build(e.status(), e.code(), e.getMessage(), e.retryAfterSeconds());
  }

  @ExceptionHandler(MissingRequestHeaderException.class)
  public ResponseEntity<ObjectNode> missingHeader(MissingRequestHeaderException e) {
    return build(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", "missing required header", null);
  }

  @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
  public ResponseEntity<ObjectNode> method(HttpRequestMethodNotSupportedException e) {
    return build(HttpStatus.METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED", "method not allowed", null);
  }

  @ExceptionHandler(NoResourceFoundException.class)
  public ResponseEntity<ObjectNode> noResource(NoResourceFoundException e) {
    return build(HttpStatus.NOT_FOUND, "NOT_FOUND", "no such resource", null);
  }

  @ExceptionHandler(FailPointException.class)
  public ResponseEntity<ObjectNode> failPoint(FailPointException e) {
    log.warn("{}", e.getMessage());
    return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "internal error", null);
  }

  @ExceptionHandler(Exception.class)
  public ResponseEntity<ObjectNode> unexpected(Exception e) {
    // Class name only: the message could contain request data.
    log.error("unexpected error: {}", e.getClass().getName());
    return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", "internal error", null);
  }

  private ResponseEntity<ObjectNode> build(
      HttpStatus status, String code, String message, Integer retryAfterSeconds) {
    ObjectNode error = mapper.createObjectNode();
    error.put("code", code);
    error.put("message", message);
    ObjectNode body = mapper.createObjectNode();
    body.set("error", error);
    ResponseEntity.BodyBuilder response =
        ResponseEntity.status(status).contentType(MediaType.APPLICATION_JSON);
    if (retryAfterSeconds != null) {
      response.header("Retry-After", String.valueOf(retryAfterSeconds));
    }
    return response.body(body);
  }
}
