package io.latchkey.orders;

import org.springframework.context.annotation.Profile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.latchkey.auth.AuthInterceptor;
import io.latchkey.auth.Tenant;
import io.latchkey.web.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Generic recovery endpoint: a client that lost the response can look the operation up. */
@Profile("api")
@RestController
@RequestMapping("/v1")
public class OperationController {

  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;
  private final InventoryStatusClient inventoryStatus;

  public OperationController(
      JdbcTemplate jdbc, ObjectMapper mapper, InventoryStatusClient inventoryStatus) {
    this.jdbc = jdbc;
    this.mapper = mapper;
    this.inventoryStatus = inventoryStatus;
  }

  @GetMapping("/operations/{operationId}")
  public JsonNode get(HttpServletRequest request, @PathVariable String operationId) {
    Tenant tenant = (Tenant) request.getAttribute(AuthInterceptor.TENANT_ATTRIBUTE);
    UUID id;
    try {
      id = UUID.fromString(operationId);
    } catch (IllegalArgumentException e) {
      throw notFound();
    }
    List<ObjectNode> rows;
    try {
      // Tenant-scoped: another tenant's operation id is indistinguishable from a missing one.
      rows =
          jdbc.query(
              "SELECT operation_id, status, response_code, response_body::text, created_at"
                  + " FROM idempotency_records WHERE operation_id = ? AND tenant_id = ?",
              (rs, i) -> {
                ObjectNode node = mapper.createObjectNode();
                node.put("operation_id", rs.getString("operation_id"));
                node.put("status", rs.getString("status"));
                int code = rs.getInt("response_code");
                if (rs.wasNull()) {
                  node.putNull("response_code");
                } else {
                  node.put("response_code", code);
                }
                String body = rs.getString("response_body");
                try {
                  if (body == null) {
                    node.putNull("response_body");
                  } else {
                    node.set("response_body", mapper.readTree(body));
                  }
                } catch (java.io.IOException e) {
                  throw new IllegalStateException("stored response is not valid JSON", e);
                }
                node.put("created_at", rs.getTimestamp("created_at").toInstant().toString());
                return node;
              },
              id,
              tenant.tenantId());
    } catch (DataAccessException e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", "service temporarily unavailable", 1);
    }
    if (rows.isEmpty()) {
      throw notFound();
    }
    return rows.get(0);
  }

  @GetMapping("/operations/{operationId}/inventory")
  public JsonNode inventory(HttpServletRequest request, @PathVariable String operationId) {
    Tenant tenant = (Tenant) request.getAttribute(AuthInterceptor.TENANT_ATTRIBUTE);
    UUID id;
    try {
      id = UUID.fromString(operationId);
    } catch (IllegalArgumentException e) {
      throw notFound();
    }

    List<OperationInventory> rows;
    try {
      rows =
          jdbc.query(
              "SELECT r.status, r.response_body ->> 'order_id' AS order_id,"
                  + " o.event_id, o.published_at IS NOT NULL AS published,"
                  + " ord.sku, ord.quantity"
                  + " FROM idempotency_records r"
                  + " LEFT JOIN orders ord ON ord.operation_id = r.operation_id"
                  + " LEFT JOIN outbox_events o ON o.aggregate_id = ord.order_id"
                  + " AND o.event_type = 'order.created'"
                  + " WHERE r.operation_id = ? AND r.tenant_id = ?",
              (rs, row) ->
                  new OperationInventory(
                      rs.getString("status"),
                      rs.getString("order_id"),
                      (UUID) rs.getObject("event_id"),
                      (Boolean) rs.getObject("published"),
                      rs.getString("sku"),
                      (Integer) rs.getObject("quantity")),
              id,
              tenant.tenantId());
    } catch (DataAccessException e) {
      throw new ApiException(
          HttpStatus.SERVICE_UNAVAILABLE, "SERVICE_UNAVAILABLE", "service temporarily unavailable", 1);
    }
    if (rows.isEmpty()) {
      throw notFound();
    }

    OperationInventory operation = rows.get(0);
    ObjectNode result = mapper.createObjectNode();
    result.put("operation_id", id.toString());
    result.put("operation_status", operation.status());
    if (operation.eventId() == null) {
      result.put("event_status", "NOT_CREATED");
      result.put("inventory_status", "NOT_APPLICABLE");
      return result;
    }

    result.put("order_id", operation.orderId());
    result.put("event_id", operation.eventId().toString());
    result.put("event_status", operation.published() ? "PUBLISHED" : "PENDING_PUBLISH");
    result.put("sku", operation.sku());
    result.put("quantity", operation.quantity());
    if (!operation.published()) {
      result.put("inventory_status", "WAITING_FOR_PUBLISH");
      return result;
    }

    InventoryStatusClient.Lookup inventory =
        inventoryStatus.lookup(UUID.fromString(operation.orderId()));
    result.put("inventory_status", inventory.status());
    if (inventory.sku() != null) result.put("inventory_sku", inventory.sku());
    if (inventory.quantity() != null) result.put("reserved_quantity", inventory.quantity());
    if (inventory.processedAt() != null) result.put("inventory_processed_at", inventory.processedAt());
    return result;
  }

  private record OperationInventory(
      String status, String orderId, UUID eventId, Boolean published, String sku, Integer quantity) {}

  private static ApiException notFound() {
    return new ApiException(HttpStatus.NOT_FOUND, "OPERATION_NOT_FOUND", "operation not found");
  }
}
