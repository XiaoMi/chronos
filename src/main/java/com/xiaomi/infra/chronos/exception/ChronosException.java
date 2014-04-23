package com.xiaomi.infra.chronos.exception;

/**
 * The normal exception for chronos.
 */
public class ChronosException extends Exception {
  
  public ChronosException(String message){
    super(message);
  }

  public ChronosException(String message, Throwable cause) {
    super(message, cause);
  }

}
