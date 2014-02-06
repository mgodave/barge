package org.robotninjas.barge;

import javax.annotation.Nonnull;

public interface Replica {

  @Override
  int hashCode();

  @Override
  boolean equals(Object o);

  @Nonnull
  @Override
  String toString();

}
