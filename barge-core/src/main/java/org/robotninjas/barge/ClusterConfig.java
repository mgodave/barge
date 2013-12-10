package org.robotninjas.barge;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

import static com.google.common.collect.Iterables.unmodifiableIterable;
import static com.google.common.collect.Lists.newArrayList;

public class ClusterConfig {

  private final Replica local;
  private final Iterable<Replica> remote;

  ClusterConfig(Replica local, Iterable<Replica> remote) {
    this.local = local;
    this.remote = remote;
  }

  public static ClusterConfig from(Replica local, Replica... remote) {
    return new ClusterConfig(local, newArrayList(remote));
  }

  public static ClusterConfig from(Replica local, Iterable<Replica> remote) {
    return new ClusterConfig(local, remote);
  }

  public Replica local() {
    return local;
  }

  public Iterable<Replica> remote() {
    return unmodifiableIterable(remote);
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

    if (!(o instanceof ClusterConfig)) {
      return false;
    }

    ClusterConfig other = (ClusterConfig) o;
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
