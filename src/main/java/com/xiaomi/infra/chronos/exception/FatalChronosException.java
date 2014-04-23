package com.xiaomi.infra.chronos.exception;

/**
 * The fatal error for chronos to provide a wrong timestamp.
 */
public class FatalChronosException extends ChronosException {

  public FatalChronosException(String message) {
    super(message);
  }

  public FatalChronosException(String message, Throwable cause) {
    super(message, cause);
  }

}
