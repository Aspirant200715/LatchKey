package io.latchkey.idempotency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class PayloadHasherTest {

  private final PayloadHasher hasher = new PayloadHasher();

  @Test
  void whitespaceDoesNotChangeHash() {
    byte[] a = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1}".getBytes());
    byte[] b = hasher.hash("create_order", "{ \"sku\" : \"A\" ,\n \"quantity\" : 1 }".getBytes());
    assertThat(a).isEqualTo(b);
  }

  @Test
  void keyOrderDoesNotChangeHash() {
    byte[] a = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1}".getBytes());
    byte[] b = hasher.hash("create_order", "{\"quantity\":1,\"sku\":\"A\"}".getBytes());
    assertThat(a).isEqualTo(b);
  }

  @Test
  void numberFormattingDoesNotChangeHash() {
    byte[] a = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1}".getBytes());
    byte[] b = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1.0}".getBytes());
    byte[] c = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1e0}".getBytes());
    assertThat(a).isEqualTo(b).isEqualTo(c);
  }

  @Test
  void unicodeEscapeAndLiteralHashTheSame() {
    byte[] a = hasher.hash("create_order", "{\"sku\":\"é\",\"quantity\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    byte[] b = hasher.hash("create_order", "{\"sku\":\"\\u00e9\",\"quantity\":1}".getBytes());
    assertThat(a).isEqualTo(b);
  }

  @Test
  void differentValueChangesHash() {
    byte[] a = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":1}".getBytes());
    byte[] b = hasher.hash("create_order", "{\"sku\":\"A\",\"quantity\":2}".getBytes());
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void operationNameIsPartOfTheHash() {
    byte[] body = "{\"sku\":\"A\",\"quantity\":1}".getBytes();
    assertThat(hasher.hash("create_order", body)).isNotEqualTo(hasher.hash("cancel_order", body));
  }

  @Test
  void hashIsSha256Length() {
    assertThat(hasher.hash("create_order", "{}".getBytes())).hasSize(32);
  }

  @Test
  void invalidJsonIsRejected() {
    assertThatThrownBy(() -> hasher.hash("create_order", "{not json".getBytes()))
        .isInstanceOf(InvalidPayloadException.class);
  }

  @Test
  void constantTimeEqualsWorks() {
    byte[] a = hasher.hash("create_order", "{\"x\":1}".getBytes());
    byte[] b = hasher.hash("create_order", "{\"x\":1}".getBytes());
    byte[] c = hasher.hash("create_order", "{\"x\":2}".getBytes());
    assertThat(PayloadHasher.matches(a, b)).isTrue();
    assertThat(PayloadHasher.matches(a, c)).isFalse();
  }
}
