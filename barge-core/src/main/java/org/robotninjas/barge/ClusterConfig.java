package org.robotninjas.barge;

public interface ClusterConfig {

  Replica local();

  Iterable<Replica> remote();

  Replica getReplica(String info);

}
