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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.robotninjas.barge.NoLeaderException;
import org.robotninjas.barge.RaftException;
import org.robotninjas.barge.Replica;
import org.robotninjas.barge.log.RaftLog;
import org.robotninjas.barge.rpc.Client;
import org.robotninjas.barge.rpc.RaftScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.addCallback;
import static org.robotninjas.barge.proto.RaftProto.*;
import static org.robotninjas.barge.state.MajorityCollector.majorityResponse;
import static org.robotninjas.barge.state.RaftPredicates.voteGranted;
import static org.robotninjas.barge.state.Raft.StateType.*;

@NotThreadSafe
class Candidate extends BaseState {

  private static final Logger LOGGER = LoggerFactory.getLogger(Candidate.class);
  private static final Random RAND = new Random(System.nanoTime());

  private final RaftLog log;
  private final ScheduledExecutorService scheduler;
  private final long electionTimeout;
  private final Client client;
  private DeadlineTimer electionTimer;
  private ListenableFuture<Boolean> electionResult;

  @Inject
  Candidate(RaftLog log, @RaftScheduler ScheduledExecutorService scheduler,
            @ElectionTimeout long electionTimeout, Client client) {
    super(CANDIDATE);
    this.log = log;
    this.scheduler = scheduler;
    this.electionTimeout = electionTimeout;
    this.client = client;
  }

  @Override
  public void init(@Nonnull final RaftStateContext ctx) {

    log.currentTerm(log.currentTerm() + 1);
    log.lastVotedFor(Optional.of(log.self()));

    LOGGER.debug("Election starting for term {}", log.currentTerm());

    List<ListenableFuture<RequestVoteResponse>> responses = sendRequests(ctx);
    electionResult = majorityResponse(responses, voteGranted());

    long timeout = electionTimeout + (RAND.nextLong() % electionTimeout);
    electionTimer = DeadlineTimer.start(scheduler, new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("Election timeout");
        transition(ctx, CANDIDATE);
      }
    }, timeout);

    addCallback(electionResult, new FutureCallback<Boolean>() {
      @Override
      public void onSuccess(@Nullable Boolean elected) {
        checkNotNull(elected);
        //noinspection ConstantConditions
        if (elected) {
          transition(ctx, LEADER);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (!electionResult.isCancelled()) {
          LOGGER.debug("Election failed with exception:", t);
        }
      }

    });

  }

  private void transition(@Nonnull RaftStateContext ctx, @Nonnull RaftStateContext.StateType state) {
    ctx.setState(this, state);
    electionResult.cancel(false);
    electionTimer.cancel();
  }

  @Nonnull
  @Override
  public RequestVoteResponse requestVote(@Nonnull RaftStateContext ctx, @Nonnull RequestVote request) {

    LOGGER.debug("RequestVote received for term {}", request.getTerm());

    Replica candidate = Replica.fromString(request.getCandidateId());

    boolean voteGranted = false;

    if (request.getTerm() > log.currentTerm()) {
      log.currentTerm(request.getTerm());
      transition(ctx, FOLLOWER);
      voteGranted = shouldVoteFor(log, request);
      if (voteGranted) {
        log.lastVotedFor(Optional.of(candidate));
      }
    }

    return RequestVoteResponse.newBuilder()
      .setTerm(log.currentTerm())
      .setVoteGranted(voteGranted)
      .build();

  }

  @Nonnull
  @Override
  public AppendEntriesResponse appendEntries(@Nonnull RaftStateContext ctx, @Nonnull AppendEntries request) {

    LOGGER.debug("AppendEntries received for term {}", request.getTerm());

    boolean success = false;

    if (request.getTerm() >= log.currentTerm()) {

      if (request.getTerm() > log.currentTerm()) {
        log.currentTerm(request.getTerm());
      }

      transition(ctx, FOLLOWER);

      success = log.append(request);

    }

    return AppendEntriesResponse.newBuilder()
      .setTerm(log.currentTerm())
      .setSuccess(success)
      .build();

  }

  @Nonnull
  @Override
  public ListenableFuture<Object> commitOperation(@Nonnull RaftStateContext ctx, @Nonnull byte[] operation) throws RaftException {
    throw new NoLeaderException();
  }

  @VisibleForTesting
  List<ListenableFuture<RequestVoteResponse>> sendRequests(RaftStateContext ctx) {

    RequestVote request =
      RequestVote.newBuilder()
        .setTerm(log.currentTerm())
        .setCandidateId(log.self().toString())
        .setLastLogIndex(log.lastLogIndex())
        .setLastLogTerm(log.lastLogTerm())
        .build();

    List<ListenableFuture<RequestVoteResponse>> responses = Lists.newArrayList();
    for (Replica replica : log.members()) {
      ListenableFuture<RequestVoteResponse> response = client.requestVote(replica, request);
      Futures.addCallback(response, checkTerm(ctx));
      responses.add(response);
    }

    return responses;
  }

  private FutureCallback<RequestVoteResponse> checkTerm(final RaftStateContext ctx) {
    return new FutureCallback<RequestVoteResponse>() {
      @Override
      public void onSuccess(@Nullable RequestVoteResponse response) {
        if (response.getTerm() > log.currentTerm()) {
          log.currentTerm(response.getTerm());
          transition(ctx, FOLLOWER);
        }
      }

      @Override
      public void onFailure(Throwable t) {}
    };
  }

}
