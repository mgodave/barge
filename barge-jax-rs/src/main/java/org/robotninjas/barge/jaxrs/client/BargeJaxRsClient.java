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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.jaxrs.Jackson;
import org.robotninjas.barge.rpc.RaftClient;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;

/**
 */
public class BargeJaxRsClient implements RaftClient {

  private final Client client;
  private final URI baseUri;

  @SuppressWarnings("UnusedDeclaration")
  public BargeJaxRsClient(URI baseUri) {
    this(baseUri, makeClient());
  }

  @VisibleForTesting
  public BargeJaxRsClient(URI baseUri, Client client) {
    this.baseUri = baseUri;
    this.client = client;
  }

  @Override
  public ListenableFuture<RequestVoteResponse> requestVote(RequestVote request) {
    final SettableFuture<RequestVoteResponse> result = SettableFuture.create();

    client.target(baseUri).path("/raft/vote")
      .request().async()
      .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), new InvocationCallback<Response>() {
        @Override
        public void completed(Response response) {
          result.set(response.readEntity(RequestVoteResponse.class));
        }

        @Override
        public void failed(Throwable throwable) {
          result.setException(throwable);
        }
      });

    return result;
  }

  @Override
  public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntries request) {
    final SettableFuture<AppendEntriesResponse> result = SettableFuture.create();

    client.target(baseUri).path("/raft/entries")
      .request().async()
      .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE), new InvocationCallback<Response>() {
        @Override
        public void completed(Response response) {
          result.set(response.readEntity(AppendEntriesResponse.class));
        }

        @Override
        public void failed(Throwable throwable) {
          result.setException(throwable);
        }
      });

    return result;
  }

  private static Client makeClient() {
    return ClientBuilder.newBuilder().register(Jackson.customJacksonProvider()).build();
  }

}
