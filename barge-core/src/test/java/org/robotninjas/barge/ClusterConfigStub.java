package org.robotninjas.barge;

import static java.util.stream.Collectors.toList;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public class ClusterConfigStub implements ClusterConfig {

  private final Replica local = new ReplicaStub("local");
  private final Iterable<Replica> remote;

  private ClusterConfigStub(Iterable<Replica> replicas) {
    remote = Lists.newArrayList(replicas);
  }

  public static ClusterConfigStub getStub(String... ids) {
    return new ClusterConfigStub(
        Lists.newArrayList(ids).stream()
            .map(ReplicaStub::new)
            .collect(toList())
    );
  }

  @Override
  public Replica local() {
    return local;
  }

  @Override
  public Iterable<Replica> remote() {
    return remote;
  }

  @Override
  public Replica getReplica(String info) {
    return new ReplicaStub(info);
  }

  private static class ReplicaStub implements Replica {

    private final String info;

    private ReplicaStub(String info) {
      this.info = info;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(info);
    }

    @Override
    public boolean equals(Object o) {

      if (o == this) {
        return true;
      }

      if (!(o instanceof ReplicaStub)) {
        return false;
      }

      ReplicaStub stub = (ReplicaStub) o;
      return Objects.equal(stub.info, info);

    }

    @Override
    public String toString() {
      return info;
    }
  }

}
