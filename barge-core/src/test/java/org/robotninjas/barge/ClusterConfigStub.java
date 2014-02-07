package org.robotninjas.barge;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

public class ClusterConfigStub implements ClusterConfig {

  private final Replica local = new ReplicaStub("local");
  private final Iterable<Replica> remote;

  private ClusterConfigStub(Iterable<Replica> replicas) {
    remote = Lists.newArrayList(replicas);
  }

  public static ClusterConfigStub getStub(String... ids) {
    return new ClusterConfigStub(FluentIterable.from(Lists.newArrayList(ids))
        .transform(new Function<String, Replica>() {
          public Replica apply(@Nullable String input) {
            return new ReplicaStub(input);
          }
        }));
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
