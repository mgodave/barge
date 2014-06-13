package org.robotninjas.barge.utils;

import java.io.File;


/**
 */
public class Files {
  @SuppressWarnings({ "ConstantConditions", "ResultOfMethodCallIgnored" })
  public static void delete(File directory) {

    for (File file : directory.listFiles()) {

      if (file.isFile()) {
        file.delete();
      } else {
        delete(file);
      }
    }

    directory.delete();
  }
}
