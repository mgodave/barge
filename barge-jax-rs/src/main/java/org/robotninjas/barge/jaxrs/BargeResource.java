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
import org.robotninjas.barge.api.AppendEntries;
import org.robotninjas.barge.api.AppendEntriesResponse;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.state.Raft;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.ExecutionException;

/**
 * Exposes a Raft instance as a REST endpoint.
 * <p>
 *   This is the server part of a Barge REST instance, exposing RPCs as endpoints. All messages are serialized through
 *   JSON.
 * </p>
 */
@Path("/raft")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class BargeResource {

  private final Raft raft;

  @Inject
  public BargeResource(Raft raft) {
    this.raft = raft;
  }

  @SuppressWarnings("UnusedDeclaration")
  @PostConstruct
  public void init() throws ExecutionException, InterruptedException {
    raft.init().get();
  }

  @Path("/state")
  @GET
  public Raft.StateType state(){
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
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Object commit(byte[] operation){
    try {
      return raft.commitOperation(operation).get();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
