package io.latchkey.orders;

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
@RestController
@RequestMapping("/v1")
public class OperationController {

  private final JdbcTemplate jdbc;
  private final ObjectMapper mapper;

  public OperationController(JdbcTemplate jdbc, ObjectMapper mapper) {
    this.jdbc = jdbc;
    this.mapper = mapper;
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

  private static ApiException notFound() {
    return new ApiException(HttpStatus.NOT_FOUND, "OPERATION_NOT_FOUND", "operation not found");
  }
}
