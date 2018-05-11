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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;
import static org.robotninjas.barge.state.Raft.StateType.CANDIDATE;
import static org.robotninjas.barge.state.Raft.StateType.FOLLOWER;
import static org.robotninjas.barge.state.Raft.StateType.LEADER;
import static org.robotninjas.barge.state.RaftPredicates.voteGranted;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import org.jetlang.fibers.Fiber;
import org.robotninjas.barge.RaftExecutor;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.api.RequestVote;
import org.robotninjas.barge.api.RequestVoteResponse;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
class Candidate extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Candidate.class);
  private static final Random RAND = new Random(System.nanoTime());

  private final Fiber scheduler;
  private final long electionTimeout;
  private final Client client;
  private DeadlineTimer electionTimer;
  private CompletableFuture<Boolean> electionResult;

  @Inject
  Candidate(RaftLog log, @RaftExecutor Fiber scheduler,
            @ElectionTimeout long electionTimeout, Client client) {
    super(CANDIDATE, log);
    this.scheduler = scheduler;
    this.electionTimeout = electionTimeout;
    this.client = client;
  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {

    final RaftLog log = getLog();

    log.currentTerm(log.currentTerm() + 1);
    log.votedFor(Optional.of(log.self()));

    LOGGER.debug("Election starting for term {}", log.currentTerm());

    List<CompletableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    // Request votes from peers
    for (Replica replica : log.members()) {
      responses.add(sendVoteRequest(ctx, replica));
    }
    // We always vote for ourselves
    responses.add(CompletableFuture.completedFuture(RequestVoteResponse.newBuilder().setVoteGranted(true).build()));

    electionResult = majorityResponse(responses, voteGranted());

    long timeout = electionTimeout + (RAND.nextLong() % electionTimeout);
    electionTimer = DeadlineTimer.start(scheduler, () -> {
      LOGGER.debug("Election timeout");
      ctx.setState(Candidate.this, CANDIDATE);
    }, timeout);

    electionResult.thenAccept(elected -> {
      checkNotNull(elected);
      //noinspection ConstantConditions
      if (elected) {
        ctx.setState(Candidate.this, LEADER);
      }
    });

    electionResult.exceptionally(t -> {
      if (!electionResult.isCancelled()) {
        LOGGER.debug("Election failed with exception:", t);
      }
      return null;
    });

  }

  @Override
  public void destroy(RaftStateContext ctx) {
    electionResult.cancel(false);
    electionTimer.cancel();
  }

  @VisibleForTesting
  CompletableFuture<RequestVoteResponse> sendVoteRequest(RaftStateContext ctx, Replica replica) {

    RaftLog log = getLog();
    RequestVote request =
      RequestVote.newBuilder()
        .setTerm(log.currentTerm())
        .setCandidateId(log.self().toString())
        .setLastLogIndex(log.lastLogIndex())
        .setLastLogTerm(log.lastLogTerm())
        .build();

    CompletableFuture<RequestVoteResponse> response =
        client.requestVote(replica, request);

    response.thenAccept(checkTerm(ctx));

    return response;
  }

  private Consumer<RequestVoteResponse> checkTerm(final RaftStateContext ctx) {
    return response -> {
      if (response.getTerm() > getLog().currentTerm()) {
        getLog().currentTerm(response.getTerm());
        ctx.setState(Candidate.this, FOLLOWER);
      }
    };
  }

}
