package io.latchkey.fault;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Deterministic crash hooks for fault-injection tests. Enabled with
 * {@code FAILPOINTS=after_commit_before_response=exit,before_commit=error}.
 * Actions: {@code exit} halts the JVM without cleanup (simulates a crash), {@code error} throws.
 * With no configuration every hit is a no-op.
 */
@Component
public class FailPoints {

  public static final String BEFORE_COMMIT = "before_commit";
  public static final String AFTER_COMMIT_BEFORE_RESPONSE = "after_commit_before_response";
  public static final String AFTER_PUBLISH_BEFORE_MARK = "after_publish_before_mark";
  public static final String AFTER_COMMIT_BEFORE_ACK = "after_commit_before_ack";

  private final Map<String, String> active = new ConcurrentHashMap<>();
  private final boolean mutable;

  public FailPoints(
      @Value("${latchkey.failpoints:}") String spec,
      @Value("${latchkey.failpoints-enabled:false}") boolean mutable) {
    this.mutable = mutable;
    if (spec != null && !spec.isBlank()) {
      for (String part : spec.split(",")) {
        String[] kv = part.trim().split("=", 2);
        active.put(kv[0], kv.length == 2 ? kv[1] : "error");
      }
    }
  }

  public void hit(String name) {
    String action = active.get(name);
    if (action == null) {
      return;
    }
    if ("exit".equals(action)) {
      Runtime.getRuntime().halt(137);
    }
    throw new FailPointException(name);
  }

  /** Runtime toggling is allowed only when latchkey.failpoints-enabled=true (demo console). */
  public boolean set(String name, String action) {
    if (!mutable) {
      return false;
    }
    if (action == null || action.isBlank()) {
      active.remove(name);
    } else {
      active.put(name, action);
    }
    return true;
  }

  public Map<String, String> snapshot() {
    return Map.copyOf(active);
  }
}
