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
package org.robotninjas.barge.jaxrs.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.Test;
import org.robotninjas.barge.jaxrs.HttpReplica;
import org.robotninjas.barge.rpc.RaftClient;

/**
 */
public class HttpRaftClientProviderTest {

  @Test
  public void providesAClientToAccessTheReplicaURLGivenAHttpReplica() throws Exception {
    HttpRaftClientProvider provider = new HttpRaftClientProvider();

    RaftClient client = provider.get(new HttpReplica(new URI("http://localhost:1234")));

    assertThat(client).isInstanceOf(BargeJaxRsClient.class);
  }
}
