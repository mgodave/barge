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

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.state.Raft;


/**
 * Exposes a Raft instance as a REST endpoint.
 * <p>
 * This is the server part of a Barge REST instance, exposing RPCs as endpoints. All messages are serialized through
 * JSON.
 * </p>
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SuppressWarnings("UnusedDeclaration")
public class BargeResource {

  private final Raft raft;
  private final ClusterConfig clusterConfig;

  @Inject
  public BargeResource(Raft raft, ClusterConfig clusterConfig) {
    this.raft = raft;
    this.clusterConfig = clusterConfig;
  }

  @Path("/init")
  @POST
  public Raft.StateType init() {

    try {
      return raft.init().get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Path("/config")
  @GET
  public ClusterConfig config(){
    return clusterConfig;
  }
  
  @Path("/state")
  @GET
  public Raft.StateType state() {
    return raft.type();
  }

  @Path("/vote")
  @POST
  public void requestVote(RequestVote vote, AsyncResponse response) {
    raft.requestVote(vote).thenAccept(response::resume);
  }

  @Path("/entries")
  @POST
  public void appendEntries(AppendEntries appendEntries, AsyncResponse response) {
    raft.appendEntries(appendEntries).thenAccept(response::resume);
  }

  @Path("/commit")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public void commit(byte[] operation, AsyncResponse response) {
      raft.commitOperation(operation).handle((i, e) -> {
        if (e instanceof NotLeaderException) {
          NotLeaderException notLeader = (NotLeaderException) e;
          response.resume(
              Response.status(Response.Status.FOUND)
                  .location(((HttpReplica) notLeader.getLeader().orElse(null)).getUri())
                  .build()
          );
        }
        response.resume(Response.noContent().build());
        return null;
      });
  }
}
