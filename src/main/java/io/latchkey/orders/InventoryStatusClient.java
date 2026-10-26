package io.latchkey.orders;

import java.util.UUID;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.http.client.SimpleClientHttpRequestFactory;

/** Calls the consumer's private Compose-network status route; never called with client-supplied IDs. */
@Profile("api")
@Component
public class InventoryStatusClient {

  private final RestClient client;

  public InventoryStatusClient(@Value("${latchkey.consumer-status.base-url:http://localhost:8083}") String baseUrl) {
    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    requestFactory.setConnectTimeout(Duration.ofMillis(500));
    requestFactory.setReadTimeout(Duration.ofSeconds(1));
    this.client = RestClient.builder().baseUrl(baseUrl).requestFactory(requestFactory).build();
  }

  public Lookup lookup(UUID orderId) {
    try {
      InventoryReservation reservation =
          client
              .get()
              .uri("/internal/inventory/orders/{orderId}", orderId)
              .retrieve()
              .body(InventoryReservation.class);
      return reservation == null ? Lookup.pending() : Lookup.reserved(reservation);
    } catch (HttpClientErrorException.NotFound e) {
      return Lookup.pending();
    } catch (RestClientException e) {
      return Lookup.unavailable();
    }
  }

  public record InventoryReservation(String status, String sku, int quantity, String createdAt) {}

  public record Lookup(String status, String sku, Integer quantity, String processedAt) {
    static Lookup pending() {
      return new Lookup("WAITING_FOR_CONSUMER", null, null, null);
    }

    static Lookup unavailable() {
      return new Lookup("CONSUMER_UNAVAILABLE", null, null, null);
    }

    static Lookup notApplicable() {
      return new Lookup("NOT_APPLICABLE", null, null, null);
    }

    static Lookup reserved(InventoryReservation reservation) {
      return new Lookup(
          reservation.status(), reservation.sku(), reservation.quantity(), reservation.createdAt());
    }
  }
}
