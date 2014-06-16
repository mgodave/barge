package org.robotninjas.barge.jaxrs;

import java.io.File;


/**
 * Simple functions for creating "unique" log directories.
 */
public class Logs {

  private static final File target = new File(Logs.class.getResource("/marker").getFile()).getParentFile();


  public static final File uniqueLog() {
    long timestamp = System.nanoTime();
    File log;

    do {
      log = new File(target, "log-" + (timestamp++));
    } while (log.exists() && log.isDirectory());

    return log;
  }
}
