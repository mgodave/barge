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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.robotninjas.barge.state.Raft;
import org.robotninjas.barge.utils.Prober;

import java.net.URI;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


/**
 */
public class JdkHttpDeploymentTest {

  @ClassRule
  public static MuteJUL muteJUL = new MuteJUL();

  private URI[] uris = new URI[3];

  private RaftJdkServer httpServer1;
  private RaftJdkServer httpServer2;
  private RaftJdkServer httpServer3;

  @Before
  public void setUp() throws Exception {
    Logger.getLogger("").setLevel(Level.ALL);

    uris[0] = new URI("http://localhost:56789/");
    uris[1] = new URI("http://localhost:56790/");
    uris[2] = new URI("http://localhost:56791/");


    httpServer1 = new RaftJdkServer(0, uris).start();
    httpServer2 = new RaftJdkServer(1, uris).start();
    httpServer3 = new RaftJdkServer(2, uris).start();
  }

  @After
  public void tearDown() throws Exception {
    httpServer1.stop(1);
    httpServer2.stop(1);
    httpServer3.stop(1);
  }

  @Test
  public void test() throws Exception {
    final Client client = ClientBuilder.newBuilder().register(Jackson.customJacksonProvider()).build();

    client.target(uris[0]).path("/raft/init").request().post(Entity.json(""));
    client.target(uris[1]).path("/raft/init").request().post(Entity.json(""));
    client.target(uris[2]).path("/raft/init").request().post(Entity.json(""));

    new Prober(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return isLeader(client, uris[0]) || isLeader(client, uris[1]) || isLeader(client, uris[2]);
        }
      }).probe(10000);

    URI leaderURI = getLeader(client);

    Response result = client.target(leaderURI)
        .path("/raft/commit")
        .request()
        .post(Entity.entity("foo".getBytes(),
            MediaType.APPLICATION_OCTET_STREAM));

    assertThat(result.getStatus()).isEqualTo(204);
  }

  private URI getLeader(Client client) {

    if (isLeader(client, uris[0]))
      return uris[0];

    if (isLeader(client, uris[1]))
      return uris[1];

    if (isLeader(client, uris[2]))
      return uris[2];

    throw new IllegalStateException("expected one server to be a leader");
  }

  private boolean isLeader(Client client, URI uri) {
    return client.target(uri).path("/raft/state").request().get(Raft.StateType.class).equals(Raft.StateType.LEADER);
  }
}
