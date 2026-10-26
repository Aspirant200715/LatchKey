package io.latchkey.consumer;

import java.util.List;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** Internal, read-only lookup used by the API demo status endpoint. */
@Profile("consumer")
@RestController
public class InventoryStatusController {

  private final JdbcTemplate jdbc;

  public InventoryStatusController(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  @GetMapping("/internal/inventory/orders/{orderId}")
  public ResponseEntity<ReservationStatus> get(@PathVariable UUID orderId) {
    List<ReservationStatus> rows =
        jdbc.query(
            "SELECT outcome, sku, quantity, created_at::text AS created_at"
                + " FROM inventory_reservations WHERE order_id = ?",
            (rs, row) ->
                new ReservationStatus(
                    rs.getString("outcome"),
                    rs.getString("sku"),
                    rs.getInt("quantity"),
                    rs.getString("created_at")),
            orderId);
    return rows.isEmpty() ? ResponseEntity.notFound().build() : ResponseEntity.ok(rows.get(0));
  }

  public record ReservationStatus(String status, String sku, int quantity, String createdAt) {}
}
