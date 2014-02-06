package org.robotninjas.barge;

public interface ClusterConfig {

  Replica local();

  Iterable<Replica> remote();

  Replica getReplica(String info);

  @Override
  int hashCode();

  @Override
  boolean equals(Object o);

  @Override
  String toString();
}
