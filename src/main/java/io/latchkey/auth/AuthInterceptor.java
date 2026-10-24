package io.latchkey.auth;

import io.latchkey.web.ApiException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Authenticates /v1/** requests. The tenant is derived from the bearer credential only; no header
 * can name a tenant. Applies a per-tenant token-bucket rate limit after authentication.
 */
@Component
public class AuthInterceptor implements HandlerInterceptor {

  public static final String TENANT_ATTRIBUTE = "latchkey.tenant";
  private static final String PREFIX = "Bearer ";

  private final TenantRepository tenants;
  private final double capacity;
  private final double refillPerSecond;
  private final Map<UUID, TokenBucket> buckets = new ConcurrentHashMap<>();

  public AuthInterceptor(
      TenantRepository tenants,
      @Value("${latchkey.rate-limit.capacity:1000}") double capacity,
      @Value("${latchkey.rate-limit.refill-per-second:1000}") double refillPerSecond) {
    this.tenants = tenants;
    this.capacity = capacity;
    this.refillPerSecond = refillPerSecond;
  }

  @Override
  public boolean preHandle(
      HttpServletRequest request, HttpServletResponse response, Object handler) {
    String header = request.getHeader("Authorization");
    if (header == null || !header.startsWith(PREFIX) || header.length() == PREFIX.length()) {
      throw unauthorized();
    }
    Tenant tenant =
        tenants.findByApiKey(header.substring(PREFIX.length()).trim()).orElseThrow(this::unauthorized);

    TokenBucket bucket =
        buckets.computeIfAbsent(
            tenant.tenantId(), id -> new TokenBucket(capacity, refillPerSecond, System::nanoTime));
    if (!bucket.tryAcquire()) {
      throw new ApiException(
          HttpStatus.TOO_MANY_REQUESTS,
          "RATE_LIMITED",
          "rate limit exceeded",
          bucket.retryAfterSeconds());
    }
    request.setAttribute(TENANT_ATTRIBUTE, tenant);
    return true;
  }

  private ApiException unauthorized() {
    return new ApiException(HttpStatus.UNAUTHORIZED, "UNAUTHORIZED", "missing or invalid API key");
  }
}
