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

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.state.Raft;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 */
public class BargeResourceTest extends JerseyTest {

  private static final ClusterConfig CLUSTER_CONFIG = HttpClusterConfig.from(new HttpReplica(uri("http://localhost:123")), new HttpReplica(uri("http://localhost:234")));
 
  @ClassRule
  public static MuteJUL muteJUL = new MuteJUL();

  private Raft raftService;


  @Test
  public void onPOSTRequestVoteReturn200WithResponseGivenServiceReturnsResponse() throws Exception {
    when(raftService.requestVote(Model.vote)).thenReturn(Model.voteResponse);

    RequestVoteResponse actual = client().target("/vote")
        .request()
        .post(Entity.entity(Model.vote, MediaType.APPLICATION_JSON_TYPE))
        .readEntity(RequestVoteResponse.class);

    assertThat(actual).isEqualTo(Model.voteResponse);
  }

  @Test
  public void onPOSTAppendEntriesReturn200WithResponseGivenServiceReturnsResponse() throws Exception {
    when(raftService.appendEntries(Model.entries)).thenReturn(Model.entriesResponse);

    AppendEntriesResponse actual = client().target("/entries")
        .request()
        .post(Entity.entity(Model.entries, MediaType.APPLICATION_JSON_TYPE))
        .readEntity(AppendEntriesResponse.class);

    assertThat(actual).isEqualTo(Model.entriesResponse);
  }

  @Test
  public void onPOSTCommitReturn204GivenServiceReturnsResponse() throws Exception {
    when(raftService.commitOperation("foo".getBytes())).thenReturn(Futures.<Object>immediateFuture("42"));

    Response value = client().target("/commit")
        .request()
        .post(Entity.entity("foo".getBytes(), MediaType.APPLICATION_OCTET_STREAM));

    assertThat(value.getStatus()).isEqualTo(204);
  }

  @Test
  public void onPOSTCommitReturn302WithLeaderURIGivenRaftThrowsNotLeaderException() throws Exception {
    URI leaderURI = new URI("http://localhost:1234");
    when(raftService.commitOperation("foo".getBytes())).thenThrow(new NotLeaderException(new HttpReplica(leaderURI)));

    Response value = client().target("/commit")
        .request()
        .post(Entity.entity("foo".getBytes(), MediaType.APPLICATION_OCTET_STREAM));

    assertThat(value.getStatus()).isEqualTo(302);
    assertThat(value.getLocation()).isEqualTo(leaderURI);
  }

  @Test
  public void onGETTypeReturnsTheCurrentStateOfRaftService() throws Exception {
    when(raftService.type()).thenReturn(Raft.StateType.LEADER);

    assertThat(client().target("/state").request().get(Raft.StateType.class)).isEqualTo(Raft.StateType.LEADER);
  }

  @Test
  public void onGETConfigReturnsCurrentClusterConfiguration() throws Exception {
    assertThat(client().target("/config").request().get(HttpClusterConfig.class)).isEqualTo(CLUSTER_CONFIG);
  }

  @Test
  public void onPOSTInitThenItSynchronouslyInitRaftService() throws Exception {
    ListenableFuture<Raft.StateType> future = Futures.immediateFuture(Raft.StateType.START);
    when(raftService.init()).thenReturn(future);

    assertThat(client().target("/init").request().post(Entity.json("")).readEntity(Raft.StateType.class)).isEqualTo(
        Raft.StateType.START);
  }

  @Override
  protected void configureClient(ClientConfig config) {
    super.configureClient(config.property(ClientProperties.FOLLOW_REDIRECTS, false).register(Jackson.customJacksonProvider()));
  }

  @Override
  protected Application configure() {
    raftService = mock(Raft.class);


    ResourceConfig resourceConfig = ResourceConfig.forApplication(new Application() {
      @Override
      public Set<Object> getSingletons() {
        return Sets.newHashSet((Object) new BargeResource(raftService, CLUSTER_CONFIG));
      }
    });

    resourceConfig.register(Jackson.customJacksonProvider());

    return resourceConfig;
  }

  private static URI uri(String uri) {
    try {
      return new URI(uri);
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

}
