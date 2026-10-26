package io.latchkey.auth;

import org.springframework.context.annotation.Profile;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Resolves a tenant from an API key. Only the SHA-256 of a key is ever stored or compared. */
@Profile("api")
@Repository
public class TenantRepository {

  private final JdbcTemplate jdbc;

  public TenantRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  public Optional<Tenant> findByApiKey(String apiKey) {
    List<Tenant> found =
        jdbc.query(
            "SELECT tenant_id, name FROM tenants WHERE api_key_sha256 = ?",
            (rs, i) -> new Tenant(rs.getObject("tenant_id", UUID.class), rs.getString("name")),
            sha256(apiKey));
    return found.stream().findFirst();
  }

  public void upsert(UUID tenantId, String name, String apiKey) {
    jdbc.update(
        "INSERT INTO tenants (tenant_id, name, api_key_sha256) VALUES (?, ?, ?) "
            + "ON CONFLICT (tenant_id) DO UPDATE SET name = EXCLUDED.name, "
            + "api_key_sha256 = EXCLUDED.api_key_sha256",
        tenantId,
        name,
        sha256(apiKey));
  }

  public static byte[] sha256(String value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }
}
