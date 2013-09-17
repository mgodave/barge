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

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.annotations.ElectionTimeout;
import org.robotninjas.barge.annotations.RaftScheduler;
import org.robotninjas.barge.context.RaftContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.robotninjas.barge.rpc.RaftProto.*;
import static org.robotninjas.barge.state.Context.StateType.CANDIDATE;

public class Follower implements State {

  private static final Logger LOGGER = LoggerFactory.getLogger(Follower.class);

  private final RaftContext rctx;
  private final ScheduledExecutorService scheduler;
  private final long timeout;
  private ScheduledFuture<?> timeoutTask;

  @Inject
  Follower(RaftContext rctx, @RaftScheduler ScheduledExecutorService scheduler, @ElectionTimeout long timeout) {
    this.rctx = rctx;
    this.scheduler = scheduler;
    this.timeout = timeout;
  }

  @Override
  public void init(Context ctx) {
    resetTimeout(ctx);
  }

  @Override
  public RequestVoteResponse requestVote(Context ctx, RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    boolean voteGranted = false;

    if (request.getTerm() >= rctx.term()) {

      if (request.getTerm() > rctx.term()) {
        rctx.term(request.getTerm());
      }

      Replica candidate = Replica.fromString(request.getCandidateId());
      voteGranted = Voting.shouldVoteFor(rctx, request);

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

    LOGGER.debug("AppendEntries received for term {}", request.getTerm());

    boolean success = false;

    if (request.getTerm() >= rctx.term()) {

      if (request.getTerm() > rctx.term()) {
        rctx.term(request.getTerm());
      }

      resetTimeout(ctx);

      success = rctx.log().append(request);

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(rctx.term())
      .setSuccess(success)
      .build();

  }

  void resetTimeout(final Context ctx) {

    if (null != timeoutTask) {
      timeoutTask.cancel(false);
    }

    timeoutTask = scheduler.schedule(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Election timeout");
        ctx.setState(CANDIDATE);
      }
    }, timeout * 2, MILLISECONDS);

  }

}
