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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.jaxrs.Jackson;
import org.robotninjas.barge.jaxrs.Model;
import org.robotninjas.barge.jaxrs.MuteJUL;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class BargeJaxRsClientTest extends JerseyTest {

  @ClassRule
  public static MuteJUL muteJUL = new MuteJUL();

  @Test
  public void returnsFutureWithServerResponseWhenRequestingVoteGivenServerAnswers() throws Exception {
    BargeJaxRsClient bargeJaxRsClient = new BargeJaxRsClient(getBaseUri(),client().register(Jackson.customJacksonProvider()));

    ListenableFuture<RequestVoteResponse> future = bargeJaxRsClient.requestVote(Model.vote);

    assertThat(future.get(1, TimeUnit.SECONDS)).isEqualTo(Model.voteResponse);
  }


  @Test
  public void returnsFutureWithServerResponseWhenAppendingEntriesGivenServerAnswers() throws Exception {
    BargeJaxRsClient bargeJaxRsClient = new BargeJaxRsClient(getBaseUri(),client().register(Jackson.customJacksonProvider()));

    ListenableFuture<AppendEntriesResponse> future = bargeJaxRsClient.appendEntries(Model.entries);

    assertThat(future.get(1, TimeUnit.SECONDS)).isEqualTo(Model.entriesResponse);
  }


  @Override
  protected Application configure() {
    ResourceConfig resourceConfig = ResourceConfig.forApplication(new Application() {
      @Override
      public Set<Object> getSingletons() {
        return Sets.newHashSet((Object) new DummyBargeServer());
      }
    });

    resourceConfig.register(Jackson.customJacksonProvider());

    return resourceConfig;
  }

  @Path("/raft")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public static class DummyBargeServer {

    @Path("/vote")
    @POST
    public RequestVoteResponse vote() {
      return Model.voteResponse;
    }

    @Path("/entries")
    @POST
    public AppendEntriesResponse entries() {
      return Model.entriesResponse;
    }
  }

}
