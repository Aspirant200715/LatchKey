package io.latchkey.idempotency;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.erdtman.jcs.JsonCanonicalizer;
import org.springframework.stereotype.Component;

/**
 * Fingerprints a request: SHA-256 over the operation name and the RFC 8785 canonical form of the
 * raw JSON body. Whitespace, key order, number formatting and unicode escapes do not change the hash.
 */
@Component
public class PayloadHasher {

  public byte[] hash(String operationName, byte[] rawBody) {
    byte[] canonical;
    try {
      canonical = new JsonCanonicalizer(rawBody).getEncodedUTF8();
    } catch (IOException | RuntimeException e) {
      throw new InvalidPayloadException("body is not valid JSON");
    }
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(operationName.getBytes(StandardCharsets.UTF_8));
      sha.update((byte) 0); // separator so name/body boundaries are unambiguous
      sha.update(canonical);
      return sha.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 unavailable", e);
    }
  }

  public static boolean matches(byte[] a, byte[] b) {
    return MessageDigest.isEqual(a, b);
  }
}
