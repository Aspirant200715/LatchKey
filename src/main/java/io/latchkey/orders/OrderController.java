package io.latchkey.orders;

import org.springframework.context.annotation.Profile;
import com.fasterxml.jackson.databind.JsonNode;
import io.latchkey.auth.AuthInterceptor;
import io.latchkey.auth.Tenant;
import io.latchkey.web.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Locale;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Profile("api")
@RestController
@RequestMapping("/v1")
public class OrderController {

  private final OrderService orders;

  public OrderController(OrderService orders) {
    this.orders = orders;
  }

  @PostMapping("/orders")
  public ResponseEntity<JsonNode> create(
      HttpServletRequest request,
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestHeader(value = "traceparent", required = false) String traceparent)
      throws IOException {
    Tenant tenant = (Tenant) request.getAttribute(AuthInterceptor.TENANT_ATTRIBUTE);

    String contentType = request.getContentType();
    if (contentType == null || !contentType.toLowerCase(Locale.ROOT).startsWith("application/json")) {
      throw new ApiException(
          HttpStatus.UNSUPPORTED_MEDIA_TYPE,
          "UNSUPPORTED_MEDIA_TYPE",
          "Content-Type must be application/json");
    }
    // Read at most MAX+1 bytes so an oversized body is detected without buffering all of it.
    byte[] body = request.getInputStream().readNBytes(OrderService.MAX_BODY_BYTES + 1);

    OrderResult result = orders.createOrder(tenant, idempotencyKey, body, traceparent);

    ResponseEntity.BodyBuilder response =
        ResponseEntity.status(result.statusCode()).contentType(MediaType.APPLICATION_JSON);
    if (result.statusCode() == 201) {
      response.header("Location", "/v1/operations/" + result.operationId());
    }
    if (result.replayed()) {
      response.header("Idempotent-Replayed", "true");
    }
    return response.body(result.body());
  }
}
