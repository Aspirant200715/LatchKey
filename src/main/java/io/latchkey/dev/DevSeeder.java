package io.latchkey.dev;

import io.latchkey.auth.TenantRepository;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Seeds two demo tenants and the SKU catalog for local runs, demos and load tests. Off by default;
 * enabled with LATCHKEY_SEED_ENABLED=true. Keys come from configuration, never from the code base.
 */
@Component
@Profile("api")
@ConditionalOnProperty(name = "latchkey.seed.enabled", havingValue = "true")
public class DevSeeder implements ApplicationRunner {

  public static final UUID TENANT_A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
  public static final UUID TENANT_B = UUID.fromString("00000000-0000-0000-0000-00000000000b");

  private static final Logger log = LoggerFactory.getLogger(DevSeeder.class);

  private final TenantRepository tenants;
  private final JdbcTemplate jdbc;
  private final String keyA;
  private final String keyB;
  private final String[] skus;

  public DevSeeder(
      TenantRepository tenants,
      JdbcTemplate jdbc,
      @Value("${latchkey.seed.key-a:}") String keyA,
      @Value("${latchkey.seed.key-b:}") String keyB,
      @Value("${latchkey.seed.skus:demo-sku}") String skus) {
    this.tenants = tenants;
    this.jdbc = jdbc;
    this.keyA = keyA;
    this.keyB = keyB;
    this.skus = skus.split(",");
  }

  @Override
  public void run(ApplicationArguments args) {
    if (keyA.isBlank() || keyB.isBlank()) {
      throw new IllegalStateException(
          "latchkey.seed.enabled=true requires latchkey.seed.key-a and latchkey.seed.key-b");
    }
    tenants.upsert(TENANT_A, "demo-a", keyA);
    tenants.upsert(TENANT_B, "demo-b", keyB);
    for (String sku : skus) {
      jdbc.update("INSERT INTO skus (sku) VALUES (?) ON CONFLICT DO NOTHING", sku.trim());
    }
    log.info("seeded 2 demo tenants and {} skus", skus.length);
  }
}
