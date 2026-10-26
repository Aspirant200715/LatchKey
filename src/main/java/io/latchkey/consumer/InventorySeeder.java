package io.latchkey.consumer;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Seeds development stock once; existing inventory is never reset on restart. */
@Component
@Profile("consumer")
@ConditionalOnProperty(name = "latchkey.inventory.seed.enabled", havingValue = "true")
public class InventorySeeder implements ApplicationRunner {

  private final JdbcTemplate jdbc;
  private final String seedSpec;

  public InventorySeeder(
      JdbcTemplate jdbc,
      @org.springframework.beans.factory.annotation.Value("${latchkey.inventory.seed.stocks:}")
          String seedSpec) {
    this.jdbc = jdbc;
    this.seedSpec = seedSpec;
  }

  @Override
  public void run(ApplicationArguments args) {
    for (Map.Entry<String, Integer> entry : parse(seedSpec).entrySet()) {
      jdbc.update(
          "INSERT INTO inventory (sku, stock) VALUES (?, ?) ON CONFLICT (sku) DO NOTHING",
          entry.getKey(),
          entry.getValue());
    }
  }

  private static Map<String, Integer> parse(String spec) {
    Map<String, Integer> stocks = new LinkedHashMap<>();
    if (spec == null || spec.isBlank()) {
      return stocks;
    }
    for (String item : spec.split(",")) {
      String[] pair = item.trim().split(":", 2);
      if (pair.length != 2 || pair[0].isBlank()) {
        throw new IllegalStateException("inventory seed entries must be sku:quantity");
      }
      int quantity;
      try {
        quantity = Integer.parseInt(pair[1]);
      } catch (NumberFormatException e) {
        throw new IllegalStateException("inventory seed quantity must be an integer", e);
      }
      if (quantity < 0 || stocks.putIfAbsent(pair[0], quantity) != null) {
        throw new IllegalStateException("inventory seed quantities must be nonnegative, unique SKUs");
      }
    }
    return stocks;
  }
}
