package org.robotninjas.barge.store;

/**
 * Thin abstraction over {@link java.lang.System}.
 */
public class Control {

  /**
   * Unconditionally shutdown the current JVM with exit code 0.
   */
  public void shutdown() {
    System.exit(0);
  }

}
