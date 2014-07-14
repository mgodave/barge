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
import org.robotninjas.barge.ClusterConfig;
import org.robotninjas.barge.NotLeaderException;
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.state.Raft;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


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
      throw Throwables.propagate(e);
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
  public RequestVoteResponse requestVote(RequestVote vote) {
    return raft.requestVote(vote);
  }

  @Path("/entries")
  @POST
  public AppendEntriesResponse appendEntries(AppendEntries appendEntries) {
    return raft.appendEntries(appendEntries);
  }

  @Path("/commit")
  @POST
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response commit(byte[] operation) {

    try {
      raft.commitOperation(operation).get();

      return Response.noContent().build();
    } catch (NotLeaderException e) {
      return Response.status(Response.Status.FOUND).location(((HttpReplica) e.getLeader()).getUri()).build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
