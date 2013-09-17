/**
 * Copyright 2013 David Rusek <dave dot rusek at gmail dot com>
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

package org.robotninjas.barge.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.context.RaftContext;
import org.robotninjas.barge.rpc.RaftClient;
import org.robotninjas.barge.rpc.RpcClientProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.robotninjas.barge.rpc.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.FOLLOWER;

public class Leader implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Leader.class);

  private final RaftContext rctx;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private final RpcClientProvider clientProvider;

  @Inject
  Leader(RaftContext rctx, @RaftScheduler ScheduledExecutorService scheduler, @ElectionTimeout long timeout,
         RpcClientProvider clientProvider) {
    this.rctx = rctx;
    this.scheduler = scheduler;
    this.timeout = timeout;
    this.clientProvider = clientProvider;
  }

  @Override
  public void init(Context ctx) {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Sending heartbeat");
        sendRequests();
      }
    }, 0, timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public RequestVoteResponse requestVote(Context ctx, RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() > rctx.term()) {

      rctx.term(request.getTerm());
      ctx.setState(FOLLOWER);

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = !rctx.votedFor().isPresent()
        || candidate.equals(rctx.votedFor().get());

      if (voteGranted) {
        rctx.votedFor(Optional.of(candidate));
      }

    }

    return RequestVoteResponse.newBuilder()
      .setTerm(rctx.term())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Override
  public AppendEntriesResponse appendEntries(Context ctx, AppendEntries request) {

    boolean success = false;

    if (request.getTerm() > rctx.term()) {
      rctx.term(request.getTerm());
      ctx.setState(FOLLOWER);
      success = rctx.log().append(request);
    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(rctx.term())
      .setSuccess(success)
      .build();

  }

  @VisibleForTesting
  List<ListenableFuture<AppendEntriesResponse>> sendRequests() {
    List<ListenableFuture<AppendEntriesResponse>> responses = Lists.newArrayList();
    for (Replica replica : rctx.log().members()) {
      RaftClient client = clientProvider.get(replica);
      AppendEntries request =
        AppendEntries.newBuilder()
        .setCommitIndex(rctx.commitIndex())
        .setPrevLogIndex(rctx.log().lastLogIndex())
        .setPrevLogTerm(rctx.log().lastLogTerm())
        .setLeaderId(rctx.self().toString())
        .setTerm(rctx.term())
        .build();
      responses.add(client.appendEntries(request));
    }
    return responses;
  }

}
