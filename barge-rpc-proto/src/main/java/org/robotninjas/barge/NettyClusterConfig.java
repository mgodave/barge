package org.robotninjas.barge;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import static com.google.common.collect.Iterables.unmodifiableIterable;
import static com.google.common.collect.Lists.newArrayList;

public class NettyClusterConfig implements ClusterConfig {

  private final NettyReplica local;
  private final Iterable<Replica> remote;

  NettyClusterConfig(NettyReplica local, Iterable<Replica> remote) {
    this.local = local;
    this.remote = remote;
  }

  public static NettyClusterConfig from(NettyReplica local, Replica... remote) {
    return new NettyClusterConfig(local, newArrayList(remote));
  }

  public static ClusterConfig from(NettyReplica local, Iterable<Replica> remote) {
    return new NettyClusterConfig(local, remote);
  }

  @Override
  public NettyReplica local() {
    return local;
  }

  @Override
  public Iterable<Replica> remote() {
    return unmodifiableIterable(remote);
  }

  @Override
  public NettyReplica getReplica(String info) {
    return NettyReplica.fromString(info);
  }

  @Override
  public int hashCode() {
    Iterable<Replica> all = Iterables.concat(newArrayList(local), remote);
    return Objects.hashCode(Iterables.toArray(all, Replica.class));
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }

    if (!(o instanceof NettyClusterConfig)) {
      return false;
    }

    NettyClusterConfig other = (NettyClusterConfig) o;
    return local.equals(other.local) &&
      Iterables.elementsEqual(remote, other.remote);

  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("local", local)
      .add("remote", remote)
      .toString();
  }
}
