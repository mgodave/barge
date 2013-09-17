package org.robotninjas.barge;

public class RaftException extends Exception {

  public RaftException() {
  }

  public RaftException(String s) {
    super(s);
  }

  public RaftException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public RaftException(Throwable throwable) {
    super(throwable);
  }
}
