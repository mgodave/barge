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

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class HttpClusterConfigTest {

  private final HttpReplica replica;
  private final HttpReplica otherReplica;

  public HttpClusterConfigTest() throws URISyntaxException {
    replica = new HttpReplica(new URI("http://foo:1234"));
    otherReplica = new HttpReplica(new URI("http://foo:1235"));
  }

  @Test
  public void returnsLocalReplicaGivenStringURIOnConfigWithLocalReplicaWithSameURI() throws Exception {
    HttpClusterConfig clusterConfig = new HttpClusterConfig(replica);

    assertThat(clusterConfig.getReplica("http://foo:1234")).isEqualTo(replica);
  }

  @Test
  public void returnsARemoteReplicaGivenStringURIOnConfigWithRemoteReplicaWithSameURI() throws Exception {
    HttpClusterConfig clusterConfig = new HttpClusterConfig(replica, otherReplica);

    assertThat(clusterConfig.getReplica("http://foo:1235")).isEqualTo(otherReplica);
  }
}
