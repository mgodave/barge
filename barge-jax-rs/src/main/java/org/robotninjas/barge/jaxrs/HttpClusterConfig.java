/**
 * Copyright 2013-2014 David Rusek <dave dot rusek at gmail dot com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.robotninjas.barge.jaxrs;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.Replica;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Objects.*;

/**
 * Configures a cluster based on HTTP transport.
 * <p>
 * A cluster configuration contains one <em>local</em> instance of a {@link HttpReplica replica} and zero or more
 * remote replicas. A configuration can be built from {@link HttpReplica} instances or simply string representing
 * URIs.
 * </p>
 */
public class HttpClusterConfig implements ClusterConfig {

  private final HttpReplica local;
  private final HttpReplica[] remotes;

  public HttpClusterConfig(HttpReplica local, HttpReplica... remotes) {
    this.local = local;
    this.remotes = remotes;
  }

  /**
   * @return the list of all replicas this particular config contains, not differentiating between local and remote instances.
   */
  public List<HttpReplica> getCluster() {
    List<HttpReplica> replicas = Lists.newArrayList(local);
    replicas.addAll(Arrays.asList(remotes));

    return replicas;
  }

  /**
   * Builds an HTTP-based cluster configuration from some replicas descriptors.
   *
   * @param local   the local replica: This is the configuration that will be used by local agent to define itself and
   *                start server endpoint.
   * @param remotes known replicas in the cluster.
   * @return a valid configuration.
   */
  public static @Nonnull ClusterConfig from(@Nonnull HttpReplica local, HttpReplica... remotes) {
    return new HttpClusterConfig(local, remotes);
  }

  @Override
  public Replica local() {
    return local;
  }

  @Override
  public Iterable<Replica> remote() {
    return Arrays.<Replica>asList(remotes);
  }

  @Override
  public Replica getReplica(String info) {
    try {
      URI uri = new URI(info);

      if (local.match(uri))
        return local;

      return Iterables.find(remote(), match(uri));
    } catch (URISyntaxException e) {
      throw new RaftHttpException(info + " is not a valid URI, cannot find a corresponding replica", e);
    }
  }

  @Override public int hashCode() {
    return Objects.hash(local, remotes);
  }

  @Override public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final HttpClusterConfig other = (HttpClusterConfig) obj;
    return Objects.equals(this.local, other.local) && Arrays.equals(this.remotes, other.remotes);
  }

  @Override public String toString() {
    return toStringHelper(this)
        .add("local", local)
        .add("remotes", Arrays.deepToString(remotes))
        .toString();
  }

  private Predicate<Replica> match(final URI uri) {
    return new Predicate<Replica>() {
      @Override
      public boolean apply(@Nullable Replica input) {
        return input != null && ((HttpReplica) input).match(uri);
      }
    };
  }

}
