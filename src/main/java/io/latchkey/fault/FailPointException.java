package io.latchkey.fault;

public class FailPointException extends RuntimeException {
  public FailPointException(String name) {
    super("fail point triggered: " + name);
  }
}
