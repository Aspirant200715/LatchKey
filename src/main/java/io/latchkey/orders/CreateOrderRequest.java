package io.latchkey.orders;

public record CreateOrderRequest(String sku, int quantity) {

  public static final int MAX_SKU_LENGTH = 64;
  public static final int MAX_QUANTITY = 1000;
}
